# JSON2 Parquet Nested Path Fallback

## 背景

当前实现会根据查询语句中对 JSON2 字段的访问方式，推导 Parquet 数据读取时所需的数据结构和类型，并在读取阶段完成必要的格式统一。

例如，执行：

```sql
select json_get(j, 'a.b') from t1;
```

时，系统会推断需要从 Parquet 中读取 JSON2 字段路径 `j.a.b`，并将该路径对应的数据转换为统一的 string 类型，以满足上层查询需求。

JSON2 的物理布局会随着写入数据的形状演进。同一个逻辑 JSON2 列在不同 SST 中可能有不同的 Parquet nested schema。例如，某些文件中 `j.a` 是 object，因此可以展开出 `j.a.x`、`j.a.y`；另一些文件中 `j.a` 可能是 scalar 或类型冲突值，因此会以 binary/jsonb 形式存储在 `j.a`。

当 Parquet 中只有 `j.a`，且类型为 jsonb/binary 时，如果查询需要读取 `j.a.b`，现有 nested projection 只会尝试读取 `j.a.b` 以及以 `j.a.b` 为前缀的子路径。由于文件中不存在这样的路径，读取计划会认为 `j.a.b` 缺失。

但从语义上看，`j.a` 中仍然可能包含查询所需的 `b` 字段。我们希望读取层能够识别这种 schema evolution 场景：在找不到精确 nested path 时，回退读取最近的 jsonb/binary 父路径，并在返回给上层前恢复成上层期望的类型和结构。

## 复现

下面是一个简单的例子，供大家复现使用。

```sql
drop table json2_schema_evolution;

create table json2_schema_evolution (
    ts timestamp time index,
    j json2
)
with (
    'append_mode' = 'true',
    'sst_format' = 'flat'
);

insert into json2_schema_evolution values
(1, '{"a": {"x": 1, "y": 2}}');

insert into json2_schema_evolution values
(3, '{"a": 1}');

admin flush_table('json2_schema_evolution');

select json_get(j, 'a.x') from json2_schema_evolution;
```

这个查询需要读取 `j.a.x`。如果某个 Parquet 文件中没有 `j.a.x`，但有 `j.a`，且 `j.a` 是 jsonb/binary，那么读取层应尝试读取 `j.a`，再从中提取 `x`，最终仍然返回上层期望的 `j.a.x` typed value。

## 目标

1. 已有精确 nested path 时，继续按现有逻辑读取，不改变行为。
2. 精确 nested path 不存在时，逐级向父路径回退，直到找到可读取的 jsonb/binary 父路径。
3. 回退读取后，对读取到的 jsonb/binary 做一次后处理，提取原始 requested path 对应的值。
4. 返回给上层的 schema 必须保持查询推导出的期望 schema，而不是暴露实际读取到的 jsonb/binary 父路径。
5. 如果没有可用的父路径，继续按缺失字段处理。

## 非目标

1. 不改变 JSON2 的持久化格式。
2. 不改变 query planner 对 JSON2 nested path 的推导方式。
3. 不在第一版中优化多个 requested paths 回退到同一个父路径的重复解析问题。
4. 不改变上层查询语义。路径缺失或类型不兼容时，仍按现有 JSON typed extraction 语义处理。

## 方案

读取阶段分成两步：

```text
Parquet projection planning
    -> Parquet physical read
    -> JSON2 fallback post-processing
    -> existing schema alignment
    -> upper reader/query
```

### 1. Projection Planning

对于每个 requested nested path，读取计划按以下顺序处理：

1. 先尝试读取 requested path 本身以及它的子路径。
2. 如果命中，则保持现有行为。
3. 如果没有命中，则从 requested path 逐级向父路径回退。
4. 只有当某个父路径是 jsonb/binary 物理列时，才允许作为 fallback 读取目标。
5. 如果所有父路径都不可用，则该 requested path 仍视为缺失。

例如：

```text
requested path: j.a.x

fallback search order:
  j.a
  j
```

如果 `j.a` 是 jsonb/binary，则读取 `j.a`，并记录这次读取是为了满足 `j.a.x`。

### 2. Fallback Post-Processing

Parquet reader 返回 physical batch 后，如果其中包含 fallback 读取结果，则在进入现有 schema alignment 之前做一次后处理：

1. 找到实际读取到的父路径，例如 `j.a`。
2. 根据原始 requested path 计算剩余路径，例如 `x`。
3. 从 `j.a` 的 jsonb/binary 值中提取 `x`。
4. 将提取出的值转换为上层期望类型。
5. 重建该 JSON2 root column，使它符合上层期望 schema。

例如：

```text
query expects:
  j.a.x as string

parquet contains:
  j.a as jsonb/binary

read:
  j.a

post-process:
  extract x from j.a
  output j.a.x as string
```

这样上层始终看到自己期望的结构，不需要感知 Parquet 中实际读取的是父路径。

### 3. Missing Path Handling

如果 requested path 没有命中，也找不到可用的 jsonb/binary 父路径，则保持现有缺失字段行为。读取层不应该为了 fallback 引入新的错误。

以下情况应返回 null 或默认缺失值：

1. 父路径不存在。
2. 父路径存在但不是 jsonb/binary。
3. jsonb/binary 中不存在 requested suffix。
4. 提取出的值无法转换为上层期望类型。
