# JSON2 Path 全文索引需求与验收

## 目标与范围

在 JSON2 显式 STRING type hint 上支持 `FULLTEXT INDEX`。
参数、默认值、查询语义及索引执行顺序均与普通列保持一致。

```sql
CREATE TABLE logs (
    ts TIMESTAMP TIME INDEX,
    attrs JSON2 (
        message STRING FULLTEXT INDEX,
        service.name STRING FULLTEXT INDEX WITH (analyzer = 'English')
    )
) WITH ('append_mode' = 'true');

SELECT * FROM logs WHERE matches_term(attrs.message, 'error');
```

- 默认使用 Bloom，可显式选择 Tantivy；每个路径按配置使用一种全文后端。
- 复用普通列对 `matches`、`matches_term`、`LIKE` 的处理及适用条件，
  不扩展后端原有能力，不增加路径专用规则。
- 支持 CREATE、ALTER hints、SHOW CREATE、持久化及重启恢复；同一路径可与
  inverted/skipping index 共存。
- flush、compaction 按当前 hints 构建索引，查询依据各 SST 的路径、类型和全文配置
  判断是否可安全使用。
- 本期不覆盖非字符串 hint、自动展开路径、已有 SST 的手动索引重建；
  修改 hints 不立即回填旧 SST。

## 简要方案

1. proto `JsonTypeHint` 增加可选全文配置，复用现有全文参数定义；缺省表示未启用，
   保留旧字段编号。同步内部类型、SQL 解析/展示与 DDL 转换。
2. 将全文索引构建、读取和缓存从列 ID 扩展到 `IndexTarget`，复用 JSON path 解析及
   叶子取值；保留普通列 blob 命名，临时目录复用 UUID 保证隔离与路径安全。
3. 扩展查询表达式识别，覆盖路径访问及现有 `lower(...)` 形式。保留原谓词复核，
   Bloom 不得漏查；旧 SST、类型或分词配置不兼容时，
   遵循现有全文查询语义安全回退，不能直接当作无匹配。
4. 基于 skipping index 分支增量开发；proto 在本地独立分支联调，提交前固定可拉取
   的远程 revision。
5. 复用普通列的索引选择与执行顺序，不新增路径专用优先级：min/max →
   Tantivy 全文 → inverted → skipping Bloom → 全文 Bloom。
   各阶段按谓词适用性执行并收窄候选行；候选为空即停止。
   沿用现有规则：Tantivy 阶段成功应用后跳过末尾全文 Bloom 阶段。

## 验收标准

- [x] 默认及自定义参数可用；非法参数、重复声明、非 STRING hint 明确报错；
      DDL/proto/元数据往返保留配置。
- [x] 相同文本及配置下，普通列与 JSON2 路径的查询结果、后端选择一致。
- [x] 两种后端与两种 batch 格式可构建路径索引；flush、compaction 后通过定向单测
      或裁剪统计证明索引实际生效。
- [x] 查询覆盖嵌套路径、含点的字面路径、多路径、NULL、缺失及空字符串；覆盖分词、
      大小写、中文和各后端支持的查询形式。
- [x] 冷/热缓存结果一致，路径之间及路径与普通列之间不串用；三类索引共存无回归。
- [x] 多索引共存时沿用普通列执行顺序，验证组合裁剪及不适用索引的跳过行为。
- [x] 新旧 SST 混读、hint 增删、类型/分词器/大小写/后端变化及重启后结果正确；不
      兼容索引不误用、不漏查。
- [x] 相关 Rust 测试、Clippy、standalone/distributed SQLness 及旧元数据升级兼容
      用例通过；自行 review 两轮。

## 实施顺序与进展

- [完成] 从 `feat/json2-skipping-index` 创建 `feat/json2-fulltext-index`；原文档
       完整备份为 `ai.md.bak`。
- [完成] 需求与方案已确认，开始按步骤实现。
- [已确认] 全文索引处理与普通列保持一致，默认 Bloom，显式可选 Tantivy。
- [完成] proto、元数据与 SQL：接入全文参数、默认 Bloom、展示及 ALTER 转换。
- [完成] 构建/查询/缓存：支持路径标识、两种后端与 batch 格式，
         读取索引结果缓存前校验 SST 的路径配置兼容性。
- [完成] 相关 Rust 回归 334 项通过，相关 crate 的 all-targets Clippy 通过；
         旧 binary → 当前 binary 的 standalone 升级兼容用例通过。
- [完成] JSON 与普通列全文 SQLness 在 standalone/distributed 均通过；
         新增组合裁剪快照已核对并复跑通过。两轮自查完成。

## 自查记录

- 第一轮发现并修复字符串表示兼容问题：谓词提取兼容 Utf8View/LargeUtf8；
  matches 统一字符串布局后求值，避免 JSON2 字符串触发类型断言。
  增加三种字符串布局、NULL 和普通列对照回归。
- 特殊路径检查：构建目录使用 UUID，Tantivy 解包缓存使用哈希键；
  单测覆盖斜杠/引号路径及分词器、大小写、后端变化后的 SST 回退。
- 第二轮检查配置兼容、缓存隔离、普通列行为及多索引组合，未发现剩余阻塞问题。
  两种全文后端均验证 flush 后裁剪 6 行、compaction 后裁剪 7 行。

proto 位于同名 `feat/json2-fulltext-index` 分支，已提交并推送到 fys；
GreptimeDB 依赖已固定到远程 revision `1af59db97330`。
远程依赖拉取及 `rtk cargo check -p api` 通过；按要求提交并推送到 fys，
不创建 PR。

## 本次追加 Review

- 第一轮：检查 SQL/proto 往返、两种后端构建、谓词提取及缓存路径隔离，
  未发现新增阻塞问题；普通列 blob 命名和执行顺序保持兼容。
- 第二轮：检查旧 SST、配置变更、NULL、多路径及组合谓词；兼容性检查位于
  结果缓存读取之前，配置不匹配时保守回退，未发现新增阻塞问题。
- 复跑相关 Rust 回归：334 项通过；核对已有 SQLness 和升级兼容测试结果。
  本轮未重跑 SQLness 或全量 workspace 测试，未修改实现代码。
- 非阻塞取舍：任一路径配置不兼容会跳过该 applier 的全部全文请求，
  影响裁剪效率。
