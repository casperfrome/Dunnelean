# Python 库

`dunnelean` 将现有 Rust 同步核心嵌入 Python 进程，提供 MySQL → Doris、Doris → MySQL 的批量同步、配置预检、任务查询和可靠取消。安装后可直接 `from dunnelean import Engine`，无需启动独立 Dunnelean HTTP 服务。数据传输继续使用 Rust 连接器和 Arrow；Python 接口只传配置和运行结果，不把表数据转换成 Python 行对象。

## 安装与构建

首版面向常规 CPython 3.10–3.14，wheel 使用 `abi3-py310`。构建目标为 Windows x86_64、Linux x86_64（manylinux2014 / glibc 2.17+）、macOS Intel 与 Apple Silicon；不包含 PyPy、32 位 Python 或 free-threaded Python。四平台的实际验证以 [Python CI](../.github/workflows/python.yml) 的运行结果为准。

项目尚未发布到 PyPI。可安装 CI 构建产物中的匹配平台 wheel，例如 Windows：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' -m pip install 'dist\dunnelean-0.1.0-cp310-abi3-win_amd64.whl'
```

安装 wheel 不需要 Rust、Maven、PyArrow 或 Python 数据库驱动。使用时仍需可连接的数据库、具备读写权限的账号，以及预先创建的目标表。

从仓库源码安装需要 `rust-toolchain.toml` 固定的 Rust 1.98.1 与本机 C/C++ 编译工具；Windows 使用 Visual Studio C++ Build Tools。项目通过 maturin 构建，pip 自动安装隔离构建依赖：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' -m pip install .
```

直接生成 wheel 与 sdist：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' -m pip install 'maturin==1.15.0'
$dunneleanBuildDir = Join-Path ([IO.Path]::GetTempPath()) ('dunnelean-build-' + [Guid]::NewGuid().ToString('N'))
& 'D:\PythonVenv\Scripts\python.exe' -m maturin build --release --locked --sdist --out dist --target-dir $dunneleanBuildDir --interpreter 'D:\PythonVenv\Scripts\python.exe'
```

`--sdist` 会将源码包解压到临时目录，再从该目录构建 wheel，检查源码包能否脱离仓库重建。每次使用新的 `--target-dir`，避免源码包规范化时间戳与已有编译缓存共同导致旧产物复用。CI 在全新 runner 上使用相同方式构建，并将 wheel/sdist 上传为 GitHub Actions artifacts；没有自动发布到 PyPI 的 job。

## 同步调用

配置继续沿用 [完整参数](parameters.md) 和现有 `examples/*.json`。接口接受 Python mapping 或 JSON 字符串，响应为普通 `dict` / `list`，字段名保持 Rust 的 `snake_case`。省略字段使用 Rust 默认值，未知配置字段被拒绝。

```python
import json
from pathlib import Path
from uuid import uuid4

from dunnelean import Engine

spec = json.loads(Path("examples/mysql-to-doris.json").read_text(encoding="utf-8"))
spec["request_id"] = str(uuid4())  # 与业务执行记录一起保存，重跑时使用新 ID

with Engine("var/python-jobs.sqlite") as engine:
    engine.validate(spec)
    submitted = engine.submit(spec)
    result = engine.wait_for_completion(submitted["run_id"], timeout=300, poll_interval=0.5)
    print(result["state"], result["rows_committed"])
    if result["state"] != "SUCCEEDED":
        print(result["error"], result["partial_write"], result["commit_unknown"])
```

`validate` 会访问数据库，检查连接、来源与目标 schema 和映射。`submit` 先检查配置并返回已持久保存的运行，数据传输在 Rust 后台继续执行；数据库预检或传输失败会记录在运行中。`run(spec, timeout=300, poll_interval=0.5)` 是提交并等待完成的便捷方法。

`FAILED`、`CANCELLED`、`INTERRUPTED` 也会作为完整运行返回；调用方按 `state/error/partial_write/commit_unknown` 判断业务结果。没有全任务回滚，取消保留已提交批次，重开状态库不会自动恢复数据传输。详细边界见 [运行语义](semantics.md)。

`password_env` 由当前 Python 进程中的 Rust 核心读取。请在创建或执行作业前设置环境变量；证书文件路径也由该进程解析，不再由另一个 Dunnelean 服务读取。不要输出包含真实密码的输入配置。

## asyncio 调用

`AsyncEngine` 用 `asyncio.to_thread` 调用释放 GIL 的原生方法；每个实例拥有独立的 Rust Tokio runtime。构造只保存参数，进入异步上下文时才打开状态库：

```python
import asyncio
import json
from pathlib import Path
from uuid import uuid4

from dunnelean import AsyncEngine

async def main():
    spec = json.loads(Path("examples/doris-to-mysql.json").read_text(encoding="utf-8"))
    spec["request_id"] = str(uuid4())
    async with AsyncEngine("var/python-async-jobs.sqlite") as engine:
        await engine.validate(spec)
        submitted = await engine.submit(spec)
        result = await engine.wait_for_completion(submitted["run_id"], timeout=300)
        print(result["state"], result["rows_committed"])

asyncio.run(main())
```

也可用 `engine = await AsyncEngine.open(state_path)`，然后在 `finally` 中 `await engine.close()`。只读属性 `state_store_id` 在打开后可直接读取，其他操作使用 `await`。

取消 `validate`、`submit` 或等待方法的 Python 协程只结束该次等待。已开始的 `to_thread` 调用可能仍在执行；取消 `submit` 的 await 后，也可能已经创建运行。保存稳定的 `request_id`，再通过 `get_request` 核实结果。停止数据传输必须显式调用 `cancel_run` 或 `cancel_request`。

打开工厂或进入异步上下文被取消时，库会在原生实例构造完成后启动后台清理，释放状态库；清理完成前同一路径仍可能报锁冲突。

异步 `validate` / `submit` 在协程开始执行时将 mapping 序列化为 JSON，再交给后台线程；此后修改原 dict 不会改变该次请求。

## 接口

构造参数单位为秒；`state_path` 必须显式提供：

```python
Engine(state_path, *, max_running=2, max_queued=16, shutdown_timeout=60)
AsyncEngine(state_path, *, max_running=2, max_queued=16, shutdown_timeout=60)
```

`max_running` 为 1–64，`max_queued` 为 0–4096。这里的并发设置控制同时执行的作业数；单个作业的 Reader/Writer 并发和内存设置仍使用配置中的 `execution`、`split` 与 `writer.options`。

下列实例方法同时存在于 `Engine` 和 `AsyncEngine`，后者需 `await`：

| 方法 | 结果与用途 |
| --- | --- |
| `ready()` | 检查实例及状态库，成功返回 `None` |
| `validate(spec)` | 返回预检结果 dict：`valid/source_schema/target_schema/semantics` |
| `submit(spec)` | 返回已提交运行 dict |
| `run(spec, *, timeout=300, poll_interval=0.5)` | 提交一次并等待任意终态 |
| `get_run(run_id)` / `cancel_run(run_id)` | 返回运行 dict；取消结果可能仍为 `CANCELLING` |
| `list_runs(limit=50, offset=0)` | 返回运行 list，最新运行在前，limit 为 1–500 |
| `list_batches(run_id)` | 返回批次凭据 list |
| `get_request(request_id)` / `cancel_request(request_id)` | 返回 `request_id/cancel_requested/run` dict；run 可为 `None` |
| `wait_for_completion(run_id, *, timeout=300, poll_interval=0.5)` | 等待任意终态，期限耗尽抛 `WaitTimeoutError` |
| `close(*, timeout=None)` | 取消活动作业并等待收尾；`None` 使用构造时的 `shutdown_timeout` |

`state_store_id` 为只读属性。模块还提供 `connectors()`、`request_schema()` 和 `__version__`；获取 schema 与连接器能力不需要打开状态库或连接数据库。

运行保留 `run_id/request_id/state/stage`、RFC3339 时间字符串、整数计数、`partial_write/commit_unknown`、可空 `error` 和脱敏 `config`。Python 整数保留 u64 精度。`rows_committed` 是确认提交的输入行数，`server_affected_rows` 是数据库统计，MySQL upsert 时二者可能不同。

SQL 绑定参数的 `i64/u64/decimal` 值与分片整数边界在 JSON 中保持字符串；二进制参数使用 Base64。`Run.config` 只用于审计，不能作为真实配置重新提交。批次 `detail` 保持可空字符串，内容为数据库凭据或错误诊断，不包含表数据。

## 幂等、取消和错误

同一个 `request_id` 与同一完整配置只创建一个运行；不同配置会报 `CONFLICT`。ID 为 1–200 UTF-8 字节。重用 ID 会返回原运行，包括已失败、取消或中断的运行；执行新一轮同步需要新 ID。幂等 ID 防止重复创建运行，不是业务数据去重键。

```python
with Engine("var/python-jobs.sqlite") as engine:
    status = engine.get_request(saved_request_id)
    engine.cancel_request(saved_request_id)
    if status["run"] is not None:
        result = engine.wait_for_completion(status["run"]["run_id"])
```

request 取消会持久保存记录，即使作业尚未提交也有效；迟到提交将报 `REQUEST_CANCELLED`。没有运行也没有取消记录时，`get_request` 报 `NOT_FOUND`。状态库身份在重开后保持不变，应与执行记录一起保存。

库不会自动重试提交或自动恢复历史运行。提交结果丢失时，先用 request ID 查询同一状态库。状态库变更、记录缺失或 `commit_unknown=true` 时，需要检查目标数据库和批次凭据，不能据此直接重放整个作业。

```python
from dunnelean import DunneleanError, WaitTimeoutError

try:
    result = engine.wait_for_completion(run_id, timeout=30)
except WaitTimeoutError as error:
    print(error.run_id, error.timeout)
    current = engine.get_run(run_id)  # 等待超时没有取消作业
except DunneleanError as error:
    print(error.code, error.message, error.commit_unknown, error.retryable)
```

`DunneleanError` 保留 Rust 的四个错误字段。`WaitTimeoutError` 使用 `WAIT_TIMEOUT`，并提供 `run_id/timeout`；`CloseTimeoutError` 使用 `CLOSE_TIMEOUT`，并提供 `timeout`。`retryable` 只描述相应操作的错误，不授权重新执行整个同步作业。

## 生命周期与状态库

一个状态库同一时间只有一个拥有者，包括另一个 Python 实例或独立 Dunnelean 服务。使用不同路径隔离应用任务，例如 Python 的 `var/python-jobs.sqlite` 与 HTTP 服务的 `var/dunnelean.sqlite`。文件锁冲突报 `STATE_STORE`。

关闭遵循 `Open → Closing → Closed`：首次 `close` 禁止新作业、发送取消并等待收尾，成功后释放 runtime 和状态库锁。重复关闭已完成实例安全返回。查询和取消可在关闭超时后的 `Closing` 状态继续使用，提交和验证报 `ENGINE_CLOSING`；关闭完成后，除重复 `close` 外的实例方法报 `ENGINE_CLOSED`，缓存的 `state_store_id` 仍可读取。

```python
from dunnelean import CloseTimeoutError

try:
    engine.close(timeout=5)
except CloseTimeoutError:
    # 资源和状态库锁继续保留，可查询、取消，再次等待关闭。
    current = engine.list_runs()
    engine.close(timeout=60)
```

上下文退出会调用 `close`，包括等待超时或业务代码抛异常后离开上下文的情况。因此等待方法本身不会取消作业，而离开 `with` / `async with` 会请求取消仍活动的作业。关闭期间已发出的数据库提交仍等待结果或超时，取消不能撤销已确认批次。

取消 `await engine.close()` 后，已启动的关闭线程仍继续收尾；再次 `await engine.close()` 可确认资源已释放。如果关闭仍超时，状态库锁会继续保留。

请显式关闭实例；GC 的非阻塞后台清理只是兜底。进程被终止时不能保证清理完成，下一次打开状态库会将旧非终态运行标成 `INTERRUPTED`，保留可能的未知提交标志。实例不能跨 `fork` 使用，子进程需创建新实例并使用独立状态路径。

## 示例与验证

先安装库，准备目标表和当前 Python 进程的密码环境变量。仓库根目录执行：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' examples/python/sync.py examples/mysql-to-doris.json
& 'D:\PythonVenv\Scripts\python.exe' examples/python/async_example.py examples/doris-to-mysql.json
& 'D:\PythonVenv\Scripts\python.exe' examples/python/sync.py examples/mysql-to-doris.json --validate-only
& 'D:\PythonVenv\Scripts\python.exe' examples/python/async_example.py examples/mysql-to-doris.json --cancel
```

示例打印 request ID、状态库身份、运行 ID 与结果；`--request-id` 可传入保存的业务执行 ID。默认状态文件与 HTTP 服务分开，`--state` 可覆盖。`--validate-only` 仍需要数据库连接，`--help` 不打开状态库。

测试已安装的 wheel，而不是源码路径中的包：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' -m pip install pytest pytest-asyncio
& 'D:\PythonVenv\Scripts\python.exe' -I -m pytest sdk/python/tests -q
```

常规测试使用临时 SQLite 和本地模拟 TCP 端点，无需启动 MySQL/Doris；覆盖状态锁、幂等、取消记录、配置契约、GIL、asyncio 等待及关闭。CI 在仓库外导入已安装包，对四平台分别测试 Python 3.10–3.14。

真实数据库验收只面向项目独立的 `dunnelean_test` 测试环境。先按 README 启动并初始化测试数据库，再运行：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' scripts/integration-python.py
& 'D:\PythonVenv\Scripts\python.exe' scripts/integration-python.py --help
```

脚本加载 `DUNNELEAN_TEST_PASSWORD` 或项目 `deploy/.env`，创建本次独有的 `py_native_<uuid>_*` 测试表并在结束后清理，验证双向同步、幂等、取消与不确定提交。报告默认保存到 `var/acceptance-python.json`，可用 `--report` 指定路径。脚本直接调用嵌入式库。

### 已完成的本地实测

2026-10-02，在 Windows x86_64 / CPython 3.10.11 上，从 sdist 重新构建并安装 release wheel，以仓库外工作目录和 `-I` 验证：30 项 pytest 全部通过，5 项真实 MySQL/Doris 验收全部通过。取消回归保留 Task、`CancelledError` traceback 和 Engine 强引用，确认打开、上下文进入及关闭的清理不依赖 GC；取消提交后可通过 request ID 找回运行。

| 真实数据库场景 | 结果 |
| --- | --- |
| 同步 MySQL → Doris | `SUCCEEDED`，47 行及类型精确校验 |
| 异步 Doris → MySQL | `SUCCEEDED`，47 行精确往返 |
| Doris 已提交后延迟凭据并取消 | `CANCELLING → CANCELLED`，保留已确认的 2 行，仅一次 PUT |
| MySQL 实际 COMMIT 后丢失回复 | `FAILED`、`commit_unknown=true`，目标存在 2 行，仅一次 COMMIT，没有重放 |
| 取消 asyncio 等待协程 | 运行继续；显式取消执行后达到 `CANCELLED` |

真实数据库结果保存在本机 `var/acceptance-python.json`。Linux、macOS 和其余 Python 版本的结果仍待四平台 CI 实际运行验证。
