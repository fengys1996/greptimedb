假设写入一下 Json 数据。

```JSON
true
{"a": true}
{"a": [1, 2, 3]}
{"a": {"b": 123}}
{"a": {"b": {"c": "x"}}}
[
    {"e": 1, "f": 2},
    {"e": 2, "h": 3},
    {"j": 3, "k": 6}
]
```

目前的 json2 format 大致为

| `__json_plain__` |
|---|
| `true` |
| `{"a": true}` |
| `{"a": [1, 2, 3]}` |
| `{"a": {"b": 123}}` |
| `{"a": {"b": {"c": "x"}}}` |
| `[{"e": 1, "f": 2}, {"e": 2, "h": 3}, {"j": 3, "k": 6}]` |


要改为

| a(serde json) | a.b | a.c | e | f | __greptime_root(serde json) | __greptime_share | __greptime_raw(jsonb) |
|---|-----|-----|---|---|----------------------------------|------------------|--------------------------|
| null | null | null | null | null | true | null | true |
| [1, 2, 3] | null | null | null | null | null | null | {"a": true} |
| true | null | null | null | null | null | null | {"a": true} |
| null | 123 | null | null | null | null | null | {"a": {"b": 123}} |
| null | null | "x" | null | null | null | null | {"a": {"b": {"c": "x"}}} |
| null | null | null | [1, 2, null] | [2, null, null] | null | [ {}, {"h": 3}, {"j": 3, "k": 6} ] | [ {"e": 1, "f": 2}, {"e": 2, "h": 3}, {"j": 3, "k": 6} ] |

但是后面两列 __greptime_share 和 __greptime_raw 可以先忽略，这是后面要改的。
