# 参数手册

JSON 字段使用 snake_case。未列出的字段一律拒绝。`null` 与省略仅对可选字段等价。连接信息由每次请求携带，不建立应用层数据源目录。

## 服务配置 dunnelean.toml

| 字段 | 默认值 | 含义 |
|---|---|---|
| listen | `127.0.0.1:9876` | 本地 HTTP 监听地址；默认无远程认证层 |
| state_path | `var/dunnelean.sqlite` | SQLite WAL 状态库，路径相对服务工作目录 |
| max_running | 2 | 并行运行任务数，1–64 |
| max_queued | 16 | 等待执行的任务数，0–4096；满时返回 429 |
| shutdown_timeout_ms | 60000 | Ctrl+C 后等待在途操作收尾的毫秒数 |

## 请求顶层

| 字段 | 必填/默认 | 含义 |
|---|---|---|
| request_id | 可选 | 1–200 字节幂等请求标识，重跑需新值 |
| reader | 必填 | `type=mysql` 或 `type=doris` |
| writer | 必填 | `type=doris` 或 `type=mysql`，必须与 Reader 对应另一个方向 |
| mapping | `[]` | `[{"source":"old_name","target":"new_name"}]`；空数组按来源列同名映射；不支持两个来源写同一目标 |
| execution | 下表默认值 | 任务资源和限速 |

### execution

| 字段 | 默认 | 单位/含义 |
|---|---|---|
| timeout_ms | 3600000 | 执行超时，包含预检；超时请求停止并等待在途提交核实 |
| queue_capacity | 4 | 待写 Arrow 批次数上限，1–65536 |
| memory_bytes | 268435456 | 内部工作缓冲预算；一半 Reader，一半排队/转换/编码；不是操作系统 RSS 硬上限 |
| max_row_bytes | 25165824 | 单行大小硬上限，含保守估算开销；超限报错 |
| rows_per_second | 不限 | 按整个任务累计行数限速，正整数 |
| bytes_per_second | 不限 | 按整个任务估算字节限速，正整数 |

批次受行数与字节数两个条件约束，先到者封批。单行超过批次软上限时可以独占一批；仍受 `max_row_bytes` 和实际内存预约约束。提高 Reader/Writer 并行度时需要相应增加内存；预算不足时背压阻塞，不无限创建缓冲。

## 共享连接结构

### credentials

`username` 必填；`password` 与 `password_env` 必须且只能提供一个。无密码也须显式 `"password":""`。`password_env` 为服务进程环境变量名。API 回显与状态记录将 `password` 替换为 `[REDACTED]`。

### tls / http_tls

| 字段 | 默认 | 含义 |
|---|---|---|
| enabled | false | 开启 TLS，证书和主机名均校验 |
| ca_certificate | 系统信任 | CA 文件路径 |
| client_certificate | 无 | 客户端证书 PEM 文件 |
| client_key | 无 | 客户端私钥文件，必须与证书同时提供 |
| server_name | 无 | Flight TLS 的 SNI/证书域名；MySQL 与 HTTP 采用连接主机名 |

关闭 TLS 时不能配置证书参数。Flight 的 `grpc+tls://`/`https://`、HTTP 的 `https://` 必须与 `enabled=true` 一致。

### timeouts

`connect_ms=10000`、`read_ms=60000`、`write_ms=120000`，均为正整数毫秒。read_ms 包括执行读取查询及等待下一个结果；write_ms 用于写入请求、SQL、提交或状态核实。

### MySQL 连接（reader.connection、writer.connection、Doris writer.sql）

| 字段 | 必填/默认 | 含义 |
|---|---|---|
| host / port | `127.0.0.1` / 3306 | Doris SQL 连接显式填 9030 |
| database | 必填 | 库名 |
| credentials | 必填 | 上述凭据结构 |
| tls / timeouts | 默认结构 | TLS 与操作超时 |
| charset | `utf8mb4` | v1 固定传输字符集；MySQL 将其他文字字符集转码为 UTF-8，Binary/BLOB 不转码 |
| time_zone | `+00:00` | 固定偏移 `±HH:MM`；不接受 IANA 时区名 |
| session_variables | `{}` | 每个新连接设置的会话变量，值为字符串；Doris 例如 `enable_decimal256: "true"` |
| max_allowed_packet_bytes | 67108864 | 客户端包上限，写入时同时受服务器真实上限约束 |

MySQL 会话固定严格 SQL 模式与显式事务规则。`session_variables` 不能覆盖 `sql_mode`、`autocommit`、`time_zone` 或字符集变量，避免破坏已声明语义。

## Reader

### source（两个 Reader 共享）

| 字段 | 默认/互斥 | 含义 |
|---|---|---|
| table | 与 query 二选一 | 当前连接数据库中的单张表名，标识符自动引用 |
| columns | `[]` | 表模式投影列名；空为全部列。表达式使用 query |
| where | 无 | 表模式的完整 SQL 条件片段，不自动添加业务增量逻辑 |
| query | 与 table/columns/where 互斥 | 完整单条 SELECT/WITH 查询 |
| params | `[]` | 仅 MySQL，按 `?` 顺序绑定；query 和 where 均可使用 |
| column_types | `{}` | 可选类型断言：输出列名 → Arrow Debug 类型字符串，如 `UInt64`；不强制转换、不静默覆盖真实元数据 |

参数使用显式类型避免 JSON 大整数/小数失真：`{"type":"u64","value":"18446744073709551615"}`。类型为 `null`（无 value）、`string`、`i64`、`u64`、`decimal`、`f64`、`bool`、`binary`。i64/u64/decimal 的 value 是字符串，binary 是 Base64。

`batch` 默认 `{"rows":10000,"bytes":16777216}`，字段均大于 0；bytes 不超过 memory_bytes 的四分之一。Reader/Writer 的批次限制分别生效，最终写入不会超过 Writer 限制。

### MySQL Reader 附加字段

- `connection`：MySQL 连接，必填。
- `consistency`：`snapshot`（默认）或 `statement`。snapshot 在每个读取连接使用只读 REPEATABLE READ 事务；statement 使用数据库单语句读取语义。
- `split`：默认无；结构为 `column`、`lower_bound`、`upper_bound`、`partitions`、`parallelism=1`。仅表模式物理整数列；bounds 使用十进制字符串，partitions 1–1024，parallelism 1–32。
- bounds 用于计算分片步长，首尾分片扩展到两端，NULL 纳入首片；不是数据过滤条件。需要限制抽取范围时使用 where。多个连接不共享同一快照，不保证全局行顺序。

### Doris Reader 附加字段

| 字段 | 必填/默认 | 含义 |
|---|---|---|
| flight_uri | 必填 | FE Flight 入口，如 `grpc://127.0.0.1:8070` |
| database / credentials | 必填 | Doris 数据库及账号 |
| tls / timeouts | 默认结构 | Flight TLS、超时 |
| session_variables | `{}` | 在 Flight 会话中执行 SET；不与 SQL 控制连接混用 |
| endpoint_map | `{}` | FE 返回的完整 Flight location URI → 宿主可达 URI |
| endpoint_parallelism | 1 | 消费返回 endpoints 的最大并行度，1–32；不会自行拆分 SQL |
| max_grpc_message_bytes | 67108864 | 单个 gRPC 接收消息上限，最多 memory_bytes/4；需要为解码预约两倍 Reader 工作区 |

Doris 4.1.4 不提供本工具所需的 Flight 参数绑定；`params` 非空会被拒绝。读取包括全部 endpoints，各 location 是同一 endpoint 的可替代地址，不能重复读取。

## Writer

两个 Writer 均有必填 `table` 和可选 `options`。

### options

| 字段 | 默认 | 含义 |
|---|---|---|
| batch | 10000 行 / 16 MiB | 批次软上限 |
| parallelism | 1 | 写入 worker 数，1–32；并行不保证顺序 |
| retry_attempts | 2 | 初始尝试之后的最大重试次数，0–16；只适用于可证明未提交的瞬态失败 |
| retry_backoff_ms | 250 | 重试退避基值；不能用重试掩盖不确定提交 |
| pre_sql | `[]` | 运行一次、数组内顺序执行；实际写入前执行并重新校验目标 |
| post_sql | `[]` | 全部数据确认后执行一次；失败仍使任务失败并保留写入计数 |

前后置 SQL 运行在独立控制连接，不是每个 worker 的会话初始化机制；会话变量请用 session_variables。每个数组元素是一条 SQL；不要依赖多语句字符串。目标表须在预检时已经存在，表结构变化在运行中重新校验。

### MySQL Writer

`connection` 必填。`mode=insert`（默认）或 `upsert`。upsert 必须提供 `key_columns`，按索引顺序匹配现存主键/唯一索引；`update_columns=[]` 默认更新所有映射的非键列，显式配置时必须是映射列且不含键列。数据库的其他唯一约束仍会参与冲突判断。只接受 InnoDB 表。

### Doris Writer

| 字段 | 必填/默认 | 含义 |
|---|---|---|
| sql | 必填 | FE MySQL 协议连接，供结构查询及前后置 SQL；认证也用于 HTTP |
| fe_http_urls | 必填非空数组 | FE HTTP 地址及 label 状态查询入口 |
| be_http_urls | `[]` | 非空时直接向 BE 导入；否则向 FE 导入并处理 307 |
| endpoint_map | `{}` | 重定向源 origin → 可达 HTTP origin；未知重定向地址报错 |
| http_tls | 默认结构 | HTTP TLS 配置 |
| mode | `append` | append 对应 Duplicate Key；upsert 对应 Unique Key Merge-on-Write，要求完整可写列 |
| label_prefix | `dunnelean` | 1–40 个 ASCII 字母/数字/下划线；实际 label 加 run_id 和批号 |
| load_timeout_seconds | 120 | 传给 Doris 的导入超时（秒）；与客户端 write_ms 不同 |
| strict_mode | true | Doris 严格导入 |
| max_filter_ratio | 0 | 允许过滤行比例，范围 [0,1]；非零时过滤计数可查询，响应丢失无法获知计数则明确报不确定 |
| time_zone | `+00:00` | Arrow 时间映射及 Stream Load 时区 |
| partitions | `[]` | 可选已有分区名列表 |
| exec_mem_limit | 2147483648 | Doris 导入内存限制，字节 |
| headers | `{}` | 额外 Stream Load 参数，值为字符串 |

headers 不能覆盖 format、label、认证、请求长度/类型、columns、strict_mode、max_filter_ratio、timeout、timezone、partitions、exec_mem_limit；首版也拒绝 group_commit、two_phase_commit、partial_columns、merge_type，以保持上述提交与写入语义。
