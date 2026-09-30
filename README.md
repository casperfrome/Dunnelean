# Dunnelean

使用 Rust 和 Apache Arrow 编写的本地离线批量同步服务。支持 **MySQL → Doris、Doris → MySQL**，Java 通过 HTTP API 控制作业。

```text
MySQL binary protocol → Arrow RecordBatch → Arrow IPC Stream → Doris Stream Load
Doris Flight SQL      → Arrow RecordBatch → prepared multi-row INSERT → MySQL
```

数据库驱动的行对象只存在于 MySQL 连接器边界。管道不使用 JSON/CSV 承载数据，不将整个表装入内存，不依赖 DataX 或 SeaTunnel 运行时。

## 快速启动

### 运行前提

- Git、Rust 和本机编译工具。`rust-toolchain.toml` 固定 Rust **1.98.1**，依赖由 `Cargo.lock` 锁定。
- Windows 使用 Visual Studio C++ Build Tools；本项目已在 Windows MSVC 上验收。
- Linux 需要 Rust 和 C 编译器；**Linux 编译尚未完成实测**，当前本地部署脚本针对 Windows PowerShell。
- 接入已有数据库时，需要可连接的 MySQL/Doris、具备读写权限的账号，以及预先创建的目标表。

Python 和 Docker 用于下文的可选本地测试环境；Java 示例需要 JDK 17+。

### 获取项目并启动

在 PowerShell 中执行：

```powershell
git clone https://github.com/casperfrome/Dunnelean.git
cd Dunnelean

# 若 cargo 尚未进入 PATH，使用 $env:USERPROFILE\.cargo\bin\cargo.exe。
cargo build --release --locked # 首次运行或更新代码后构建
.\scripts\run.ps1
```

`run.ps1` 在当前终端前台运行服务，按 `Ctrl+C` 停止。它只启动 Dunnelean，不调用 Docker，也不会启动 Doris 或 MySQL 容器。服务可以在数据库未启动时独立启动；验证或执行同步任务时，相关数据库需要可连接。

服务默认监听 `http://127.0.0.1:9876`，状态保存在 `var/dunnelean.sqlite`，配置文件为根目录的 `dunnelean.toml`。也可以在项目根目录直接运行已构建的程序：

```powershell
.\target\release\dunnelean.exe serve --config dunnelean.toml
```

接入已有数据库时，修改作业配置中的连接、来源和目标，目标表需要预先存在。使用 `password_env` 时，在启动服务前设置对应环境变量；环境变量由 **Dunnelean 进程**读取，不是由 Java 读取。`run.ps1` 会在 `deploy/.env` 存在时加载其中的 `DUNNELEAN_TEST_PASSWORD`；直接运行可执行程序时需自行设置该变量。

### 检查服务

保持服务终端运行，在另一个 PowerShell 窗口执行：

```powershell
Invoke-RestMethod http://127.0.0.1:9876/healthz
Invoke-RestMethod http://127.0.0.1:9876/readyz
```

成功时分别返回 `status: ok` 和 `status: ready`。这两个接口检查服务及状态库；数据库连接和作业配置需要通过 `/v1/validate` 检查。

## 可选：本地测试数据库

本地测试环境使用 Docker Desktop Linux 容器运行 Doris FE/BE **4.1.4** 和测试 MySQL **9.7.2**。启动脚本要求 Docker 至少有 11 GiB 内存、6 个逻辑 CPU、8 GiB 可用磁盘，并设置 `vm.max_map_count >= 2000000`；详细要求见 [本机部署说明](docs/deployment.md)。

需要本项目自带的测试数据库时，先在项目根目录准备 Python 环境（已验收 Python 3.10.11）：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install pymysql requests psutil

.\scripts\start-env.ps1
.\.venv\Scripts\python.exe scripts/init-test-env.py
```

`python` 应指向本机 Python 3 解释器；后续命令直接使用 `.venv`，无需激活虚拟环境。初始化脚本依赖 `pymysql`；验收脚本还依赖 `requests`、`psutil`。

`start-env.ps1` 管理独立的 `dunnelean` Compose 项目；不更改已有容器。随机开发密码保存在忽略文件 `deploy/.env`，由脚本首次运行时自动生成。`init-test-env.py` 初始化测试 Doris 数据库和业务账号。完成后按快速启动步骤启动或重启 Dunnelean，使服务加载测试密码。

**初始化数据库和账号不会创建示例表。** `examples/*.json` 使用 `source_orders`、`target_orders`、`returned_orders`；在此独立测试环境运行下文基础验收脚本可创建这些表并验证双向同步。接入已有数据库时，请将示例改为实际连接和表配置。停止测试容器使用 `.\scripts\stop-env.ps1`，数据卷保留。

## 调用

在项目根目录执行；使用原始示例前，需按上文准备对应的测试表和服务环境变量。

```powershell
$body = Get-Content examples/mysql-to-doris.json -Raw -Encoding UTF8
Invoke-RestMethod http://127.0.0.1:9876/v1/validate -Method Post -ContentType application/json -Body ([Text.Encoding]::UTF8.GetBytes($body))
$run = Invoke-RestMethod http://127.0.0.1:9876/v1/runs -Method Post -ContentType application/json -Body ([Text.Encoding]::UTF8.GetBytes($body))
Invoke-RestMethod "http://127.0.0.1:9876/v1/runs/$($run.run_id)"
Invoke-RestMethod "http://127.0.0.1:9876/v1/runs/$($run.run_id)/cancel" -Method Post
```

Java 17+ 标准库示例：

```text
javac -encoding UTF-8 examples/java/DunneleanClient.java
java -cp examples/java DunneleanClient examples/mysql-to-doris.json
java -cp examples/java DunneleanClient examples/doris-to-mysql.json --cancel
```

Java 只传配置并轮询状态，无需 Arrow/JDBC 依赖。需要可靠处理 Java 请求重试时给 JSON 添加稳定的 `request_id`。相同 ID、相同完整配置返回原运行；配置不同返回 409。已完成、失败或中断的原运行不会因重复提交而重跑；重跑使用新 ID。

## 文档与契约

- [原理与 DataX、SeaTunnel 对比](docs/principles-and-comparison.md)
- [完整参数](docs/parameters.md)
- [内核结构](docs/architecture.md)
- [数据类型与运行语义](docs/semantics.md)
- [工作台可靠控制协议](docs/studio-control.md)
- [Doris 部署说明](docs/deployment.md)
- [实测验收报告](docs/acceptance.md)
- [OpenAPI](docs/openapi.json)、[连接器 JSON Schema](docs/connectors.schema.json)
- 服务同时提供 `/openapi.json` 和 `/v1/connectors`，内容由实际 Rust 配置类型生成。

接口：`GET /healthz`、`GET /readyz`、`POST /v1/validate`、`POST /v1/runs`、`GET /v1/runs?limit=50&offset=0`、`GET /v1/runs/{id}`、`POST /v1/runs/{id}/cancel`、`GET /v1/runs/{id}/batches`、`GET /v1/requests/{id}`、`POST /v1/requests/{id}/cancel`。

## 验收

基础工程检查无需启动数据库：

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

数据库验收需要先完成本地测试环境初始化并构建 Release 程序。以下命令按顺序执行；百万行验收创建扩展测试需要的 fixtures，所有数据库验收仅用于独立的 `dunnelean_test` 测试库：

```powershell
.\.venv\Scripts\python.exe scripts/integration.py --binary target/release/dunnelean.exe --rows 1000000
.\.venv\Scripts\python.exe scripts/integration-more.py
# 千万行压力测试，只改动 dunnelean_test 中的 bench_* 测试表：
.\.venv\Scripts\python.exe scripts/integration.py --binary target/release/dunnelean.exe --only-scale --rows 10000000
# 重建本项目 FE/BE 并验证持久化（会短暂停机）：
.\.venv\Scripts\python.exe scripts/verify-persistence.py
```

验收启动独立 `9877` 端口服务，保存 `var/acceptance.json` 和服务日志。测试结束停止该服务；常规 `9876` 服务独立运行。每次压力测试重新创建自身 `bench_*` 表，不能用于业务数据库。

首版按批次提交，允许失败前已有数据写入。取消不撤销已提交数据；进程重启不自动恢复数据传输。完整边界见运行语义文档。Doris Flight SQL 官方目前仍标为实验功能，本工具固定以 4.1.4 验收。
