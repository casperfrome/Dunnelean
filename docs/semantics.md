# 数据类型、事务和失败语义

## 数据面

连接器之间只传 Arrow Schema/RecordBatch。MySQL 二进制协议的值立即追加到列 builder；Doris Flight 直接解码 Arrow。Doris Writer 每个提交批次生成一个完整 Arrow IPC **Stream**，不是 IPC File、CSV 或 JSON。

数据库边界可能要求不同的物理表示。例如 Doris LARGEINT 通过带 `doris_type=LARGEINT` 元数据的 Arrow 字符串传输；Reader 将其标准化为 Decimal256(39,0)，Writer 在验证 i128 范围后恢复其协议表示。此转换只在 Doris 连接器边界发生。

| 类型 | 内部表示及约束 |
|---|---|
| MySQL signed / unsigned 整数 | 对应 Arrow Int/UInt；无符号大整数不经 i64 或浮点中转 |
| Boolean | Arrow Boolean；MySQL TINYINT 默认仍为整数，与 Boolean 的转换仅接受 0/1 |
| DECIMAL | precision≤38 用 Decimal128，否则 Decimal256；MySQL 最大 65 位，Doris 最大 76 位受数据库配置影响 |
| Decimal scale 改变 | 必须精确；减少 scale 仅允许被删除位全为 0；不舍入 |
| DATE | Arrow Date32，零日期和非法日期失败 |
| DATETIME | 无时区 Timestamp；保留墙上时间 |
| TIMESTAMP | UTC Timestamp；在 MySQL 边界按固定会话偏移换算 |
| MySQL TIME | 有符号 Duration(Microsecond)，包括负值和超过 24 小时的时长 |
| 文字/JSON | Utf8；JSON 的数据库语义由目标列校验，不能把 JSON 空值等同于 SQL NULL |
| Binary/BLOB | Binary；不做 lossy UTF-8 解码；目标必须可表达，否则预检拒绝 |

向无时区目标写 UTC Timestamp 时按 Writer time_zone 转为对应墙上时间；反方向按同一偏移解释。跨系统往返要明确双方时区，默认均 UTC。微秒向更低精度目标写入时只允许精确可表示的值。

来源列与目标列按配置映射，目标生成列不可写；省略的目标列必须可空、有默认值或由数据库生成。不兼容的类型、重复目标映射和遗漏必填列在预检中拒绝；范围和内容相关错误在实际批次校验中拒绝。

Doris ARRAY/MAP/STRUCT、HLL、BITMAP、VARIANT 等类型不自动序列化成字符串。需要同步到 MySQL 时，调用方通过来源 SQL 明确选择 JSON/文本或其他标量投影。MySQL TIME/BLOB 向没有等价类型的 Doris 表同步时同样需要明确的来源 SQL 转换。本工具不推测编码和目标 DDL。

## 提交与状态

流程为：参数/连接/schema 预检 → Writer pre_sql → 重新校验目标 → 流式读写及批次确认 → Writer post_sql → 成功。

- MySQL 每个批次使用独立 InnoDB 事务。真正的多行 prepared INSERT 受 65,535 参数及 packet 大小约束，必要时在同一批事务内拆成多个语句。
- MySQL upsert 的 affected rows 通常为 0/1/2；`rows_committed` 记录已确认提交的输入行数，`server_affected_rows` 单独记录数据库统计。
- Doris 每个 Arrow Stream Load 批次一个 label；表模型决定追加/覆盖语义。完整行 Unique Key 更新不传播来源删除，也不删除目标独有数据。
- 没有全任务事务、影子表或自动补偿。前后置 DDL/DML 可能自行提交，post_sql 失败也不会撤回已经同步的数据。

SQLite 的 `batches` 表在写入前保存 INTENT，在数据库明确确认后将凭据和累计进度一起提交。`CONFIRMED`、`NOT_COMMITTED`、`UNKNOWN` 区分确认结果。API `/v1/runs/{id}/batches` 可查询 label 与诊断。

| 情况 | 行为 |
|---|---|
| 写入前失败/类型错误 | 停止后续工作，保留先前确认批次 |
| MySQL 已知事务回滚后的死锁/锁等待失败 | 有界重试；不扩大为整任务重跑 |
| MySQL COMMIT 响应丢失 | FAILED + commit_unknown=true，不重放 |
| Doris Success | 校验加载/过滤/未选中行数后确认 |
| Doris Publish Timeout / Label Already Exists / 响应丢失 | 查询 FE label 状态；VISIBLE 可确认，COMMITTED/PREPARE 有界等待 |
| Doris 明确 ABORTED | 允许原 label、原始 payload 有界重试 |
| Doris UNKNOWN 或无法核实 | FAILED + commit_unknown=true，不换 label 重传 |
| 已提交但本地凭据保存失败 | 保留不确定状态；不能把数据库成功当作本地失败后直接重放 |
| 进程重启 | 非终态任务标为 INTERRUPTED，未结清 INTENT 视为提交结果未知；不自动恢复 |

取消接口只表示已接收停止请求。Reader 停止继续拉取，Writer 不开始新批次，已经发出的提交继续等结果或超时。所有 worker 收尾后才写终态，已确认数据保留。任务超时采用同样的收尾流程。

幂等 request_id 仅防止同一 API 请求重复创建运行；它不是业务数据去重键。新 request_id 会重新读取来源。Doris label 去重有数据库保留期限制，因此不能把历史 label 当作无限期恢复机制。

## 一致性、顺序与内存

单连接 MySQL snapshot 使用只读 REPEATABLE READ 事务；非事务型来源表不具备 InnoDB 快照保证。启用分片后每个连接各自建立事务，不能宣称共同时间点快照。Doris 单次查询消费全部 Flight endpoints，不用 LIMIT/OFFSET 循环拼接数据集。

并发执行不保证全局行序。同一业务键有多条输入且依赖“最后一条覆盖”时，应使用单 Reader、单 Writer 和显式排序查询，并由调用方确定重复键策略。

资源控制分别约束 Reader 工作缓冲和排队/转换/编码缓冲，避免 Reader 占满所有内存后 Writer 无法前进。除了数据缓冲，驱动、TLS、SQLite、分配器等也占用内存；memory_bytes 不是进程 RSS 限额。总数据量增加只增加执行时间与持久化凭据，不增加完整表内存缓存。

## 参考协议

- [Doris Flight SQL 4.x](https://doris.apache.org/docs/4.x/connection-integration/arrow-flight-sql/)
- [Doris Stream Load](https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/)
- [Doris 4.1.4 Arrow Stream Reader](https://github.com/apache/doris/blob/4.1.4/be/src/format/arrow/arrow_stream_reader.cpp)
