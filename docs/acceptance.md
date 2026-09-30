# Dunnelean 0.1.0 验收记录

测试日期：2026-09-29。Windows 原生 Rust 1.98.1 MSVC 程序；Docker Desktop Linux，16 CPU、约 14 GiB 内存。独立 Compose 项目运行单 FE/BE Doris 4.1.4 及测试 MySQL 9.7.2。测试未使用现有 MySQL 容器。

FE/BE 使用锁定 digest 的官方 4.1.4 镜像。数据库内部版本字符串为 `doris-4.1.4-rc04-ad35a140c7f`，完整版本、持久化检查前后结果见 [环境记录](acceptance-environment.json)。

## 工程检查

- `cargo fmt --all -- --check` 通过。
- `cargo clippy --all-targets -- -D warnings` 通过。
- `cargo test --all-targets`：14 项通过。
- Release MSVC 可执行程序构建通过；Compose 配置校验通过。
- 本次实测平台为 Windows；Rust 代码采用跨平台依赖，Linux 编译未在本轮执行。

## 功能与故障

[基础实测](acceptance-functional.json)覆盖双向多批次、追加、MySQL/Doris upsert、空结果、SQL 绑定参数、过滤、字段选择/重排/改名、前后置 SQL、并行整数分片、幂等冲突、密码脱敏及 MySQL COMMIT 响应丢失。

[扩展实测](acceptance-extended.json)覆盖：

- NULL、中文、emoji、换行、UInt64 最大值、Decimal128/256、精确 scale 调整、DATE/DATETIME/TIMESTAMP 微秒、BOOLEAN/LARGEINT 往返。
- 有符号负 TIME 与二进制列在不兼容目标上明确拒绝；显式来源 SQL 转换后保留负时长微秒和二进制 HEX。减少 decimal scale 导致非零位丢失及零日期均明确失败。
- NULL 分片键及边界外整数不遗漏，读取连接被中断后任务失败。
- 数据库已实际提交后，代理丢弃 Doris HTTP 响应；通过真实 FE label 状态确认，只发出一次 PUT，不重放。
- 数据库已实际提交、响应暂缓时请求取消；保持 CANCELLING，收到确认后才结束，保留已提交计数。
- 数据库已提交后 SQLite 触发器拒绝保存凭据；任务标记提交结果未知，INTENT 保留。
- Java 标准 HttpClient 实际完成提交、轮询、重复请求去重和取消。
- 强杀服务进程后重启，遗留任务变为 INTERRUPTED，已确认进度保留，相同 request_id 不重新执行。

单元/协议测试另覆盖：慢 Writer 背压和取消释放内存、SQLite 凭据与计数的原子性、独占状态库、MySQL 负 TIME 表示、整数/浮点溢出拒绝、Doris Publish Timeout/重复 label/UNKNOWN、明确 ABORTED 后复用同一 label 和同一 IPC 请求体。

JSON 中预期故障案例的 FAILED/CANCELLED/INTERRUPTED 是验收断言的正确结果；脚本只有所有断言通过才输出报告。发布超时与重复 label 的异常分支使用可控 HTTP 协议服务模拟；实际网络响应丢失使用真实数据库加故障代理验证。

## 规模实测

| 行数 | 方向 | 用时 | 吞吐（行/秒） | Dunnelean 峰值 RSS |
|---:|---|---:|---:|---:|
| 1,000,000 | MySQL → Doris | 4.453 秒 | 224,568 | 21.2 MiB |
| 1,000,000 | Doris → MySQL | 7.719 秒 | 129,550 | 61.5 MiB |
| 10,000,000 | MySQL → Doris | 42.453 秒 | 235,555 | 21.0 MiB |
| 10,000,000 | Doris → MySQL | 73.312 秒 | 136,403 | 113.6 MiB |

采用默认单 Reader/Writer、10,000 行或 16 MiB 批次、队列 4、256 MiB 缓冲预算。数据含 BIGINT、DECIMAL(20,4)、短 VARCHAR。双向完成后对比 COUNT、SUM(id)、SUM(amount)、SUM(LENGTH(payload))；类型功能测试另外逐行比较完整往返结果。

时间包含提交与轮询至终态，不含建表和生成源数据；RSS 通过 psutil 约每 150 毫秒采样，只统计 Dunnelean 进程，不含数据库容器。这是本机单次观测，不代表其他硬件或宽行数据的吞吐保证。数据量增长十倍，RSS 没有同比增长；分配器保留与 Flight 消息大小会影响峰值。

原始记录：[百万行](acceptance-million.json)、[千万行](acceptance-ten-million.json)。

## 持久化与复现

对 FE/BE 执行强制容器重建；named volumes 保留。重建后业务账号可登录，10,000,000 行及聚合值保持一致，配置校验一致，FE/BE 和 Flight 恢复可用。脚本为 `scripts/verify-persistence.py`。

执行 README 中的验收命令可复现。所有脚本只修改独立的 `dunnelean_test` 测试表；`integration-more.py` 依赖基础测试和规模测试生成的 fixtures。千万行压力脚本已实际运行。

本轮未进行多 FE/多 BE 集群、TLS/mTLS 证书矩阵或长时间生产负载验收；配置支持这些连接参数，当前验收范围是约定的本地单 FE/BE 拓扑。
