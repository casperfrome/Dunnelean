# Go 原生库

`github.com/casperfrome/Dunnelean/sdk/go` 把 Dunnelean 的 Rust 同步核心嵌入 Go 进程，支持 MySQL → Doris、Doris → MySQL、配置预检、运行查询和取消。应用只安装 Go 包即可使用，不需要启动独立服务，也不需要安装 Python、Java、Rust 或 C 编译器。数据批次保留在 Rust Arrow 管线中，Go 只传配置和读取运行记录。

## 安装和运行环境

在自己的 Go 项目中执行：

```sh
go get github.com/casperfrome/Dunnelean/sdk/go@v0.1.0
```

引入包：

```go
import dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
```

首版要求 Go 1.25+，支持下列原生引擎与普通 Go 可执行程序：

| Go 目标 | 原生库 | 系统要求 |
| --- | --- | --- |
| `windows/amd64` | MSVC DLL，静态 CRT | Windows 10+ / Server 2016+ |
| `linux/amd64` | manylinux2014 `.so` | glibc 2.17+、Linux kernel 3.2+ |
| `darwin/amd64` | Intel `.dylib` | Go 1.25/1.26：macOS 12+；Go 1.27：macOS 13+ |
| `darwin/arm64` | Apple Silicon `.dylib` | Go 1.25/1.26：macOS 12+；Go 1.27：macOS 13+ |

首版不支持 Alpine/musl、Linux ARM64、32 位 Go、移动端或 WebAssembly。`CGO_ENABLED=0 go build` 可直接构建；跨平台编译只需要设置支持的 `GOOS` 和 `GOARCH`，最终程序在对应系统运行。Linux 程序仍需要上述系统动态库，因此部署镜像应提供 glibc。

Go module 包含四平台压缩原生库。构建时 `go:embed` 只把目标平台的引擎编入最终程序。首次打开引擎会将库释放到当前用户缓存，再在同一进程中动态加载；运行时不会下载文件或创建 Dunnelean 子进程。发布的 Go 程序不依赖 module cache 或开发机源码路径。

默认缓存位置为 `os.UserCacheDir()/dunnelean/native/<version>/<os>_<arch>/<sha256>/`。缓存目录需可写、允许加载本机代码，并支持硬链接原子发布（如 NTFS、ext4、APFS）；受限容器或默认缓存挂载为 `noexec` 时，可显式设置：

```go
engine, err := dunnelean.Open(ctx, "var/jobs.sqlite",
    dunnelean.WithNativeCacheDir("/app-cache/dunnelean"),
)
```

库会检查压缩资产与释放文件的长度、SHA-256、原生 ABI 和版本；缓存损坏时返回错误，修复或删除对应缓存后重试。缓存文件按内容命名，不覆盖正在加载的库。`Engine.Close` 释放引擎和状态库，原生库保持加载到进程退出；Windows 上删除已加载 DLL 应等相关进程退出。

启用 macOS Hardened Runtime 的签名应用还需要按应用签名策略处理内嵌 dylib 的签名与 Library Validation；普通 Go 命令行程序的 CI 验证不代表所有签名应用都可直接加载。[Apple Library Validation 说明](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.security.cs.disable-library-validation)

## 提交和等待

配置沿用 [RunSpec 参数](https://github.com/casperfrome/Dunnelean/blob/main/docs/parameters.md) 和 [JSON 示例](https://github.com/casperfrome/Dunnelean/tree/main/examples)。接口使用 `json.RawMessage`，也可由自己的 struct 或 map 通过 `json.Marshal` 得到。Go 层只检查接口参数，配置默认值、未知字段、连接和映射校验由 Rust 负责。

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "time"

    dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
)

func main() {
    ctx := context.Background()
    engine, err := dunnelean.Open(ctx, "var/jobs.sqlite")
    if err != nil { log.Fatal(err) }
    defer func() {
        closeCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
        defer cancel()
        if err := engine.Close(closeCtx); err != nil { log.Print(err) }
    }()

    data, err := os.ReadFile("mysql-to-doris.json")
    if err != nil { log.Print(err); return }
    spec := json.RawMessage(data) // JSON 内保存稳定、唯一的业务 request_id
    if _, err := engine.Validate(ctx, spec); err != nil { log.Print(err); return }
    accepted, err := engine.Submit(ctx, spec)
    if err != nil { log.Print(err); return }
    log.Printf("run=%s store=%s", accepted.RunID, engine.StateStoreID())
    result, err := engine.WaitForCompletion(ctx, accepted.RunID,
        dunnelean.WithWaitTimeout(5*time.Minute),
        dunnelean.WithPollInterval(500*time.Millisecond),
    )
    if err != nil { log.Print(err); return }
    log.Printf("state=%s committed=%d error=%v", result.State, result.RowsCommitted, result.Error)
}
```

`Open` 只创建 runtime 和持久状态库，不连接业务数据库。`Validate` 访问数据库并检查 schema 与映射。`Submit` 返回已持久化的运行，数据传输在 Rust 后台继续执行；运行中出现的连接、预检、写入或提交错误保存为运行记录。

`Run(ctx, spec, waitOptions...)` 等价于提交一次再等待。`SUCCEEDED`、`FAILED`、`CANCELLED`、`INTERRUPTED` 都是正常的终态返回：调用本身的 `error` 为 `nil`，业务失败位于 `result.Error`。调用方必须检查 `State`、`Error`、`PartialWrite` 和 `CommitUnknown`。没有全任务回滚，取消保留已经提交的批次。[完整运行语义](https://github.com/casperfrome/Dunnelean/blob/main/docs/semantics.md)

密码环境变量和证书路径由当前 Go 进程中的 Rust 核心读取。运行前给该进程设置 `password_env` 指定的变量；证书相对路径基于该进程的当前工作目录。目标数据库和表需要提前创建，账号需要相应权限。

## goroutine、context 和幂等

每个实例持有独立的 Rust Engine、SQLite Store 和多线程 Tokio runtime。阻塞原生调用不会阻止其他 goroutine 调度，`Engine` 可供多个 goroutine 并发使用。需要后台运行时，直接在 goroutine 中调用现有接口：

```go
go func() {
    result, err := engine.Run(ctx, spec)
    // 将结果传给应用自己的 channel、回调或持久记录。
    _ = result
    _ = err
}()
```

取消 `context` 或等待超时只结束该次本地等待；已开始的原生调用可能继续执行。停止同步必须显式调用 `CancelRun` 或 `CancelRequest`。`Submit` 的 context 被取消时，运行可能已经创建；保存 `request_id`，然后用新的有效 context 调用 `GetRequest` 查询同一状态库。库不自动重试提交。

`WaitForCompletion` 和 `Run` 在等待失败时返回最后观察到的 `*Run`（如果已观察到），同时返回 context 错误或 `*WaitTimeoutError`。调用方因此可以保留已接受的运行 ID，随后再查询；本地等待结束不会修改数据库作业的取消状态。

同一 `request_id` 与相同完整配置只创建一个运行；配置不同报 `CONFLICT`。ID 为 1–200 UTF-8 字节。重用 ID 返回原运行，包括已失败、取消或中断的运行；新一轮同步应使用新 ID。`CancelRequest` 会持久保存取消记录，即使尚未提交运行，迟到提交也会报 `REQUEST_CANCELLED`。

```go
status, err := engine.GetRequest(context.Background(), savedRequestID)
if err == nil && status.Run != nil {
    result, waitErr := engine.WaitForCompletion(context.Background(), status.Run.RunID)
    _ = result
    _ = waitErr
}
```

状态库身份 `StateStoreID()` 在关闭后仍可读取，重开同一库保持不变，应与业务请求记录一起保存。提交回执丢失时，先查询同一状态库；记录缺失、状态库不同或 `CommitUnknown=true` 时，需要核对目标数据库与批次凭据，不能据此重放整个作业。`RowsCommitted` 是已确认提交的输入行数，`ServerAffectedRows` 是服务器统计，二者在 MySQL upsert 时可能不同。

## 接口和关闭

| 接口 | 结果与用途 |
| --- | --- |
| `Open(ctx, statePath, ...Option)` | 创建 `*Engine`；必须显式提供持久 SQLite 文件路径 |
| `WithMaxRunning(n)` / `WithMaxQueued(n)` | 默认 2 / 16；范围 1–64 / 0–4096 |
| `WithShutdownTimeout(duration)` | 关闭等待默认 60 秒 |
| `WithNativeCacheDir(path)` | 覆盖内嵌库释放目录 |
| `engine.Ready(ctx)` | 检查实例和状态库 |
| `engine.Validate(ctx, spec)` | 返回 `*Validation` |
| `engine.Submit(ctx, spec)` / `engine.Run(ctx, spec, ...WaitOption)` | 返回 `*Run`；后者提交后等待终态 |
| `engine.GetRun(ctx, id)` / `engine.CancelRun(ctx, id)` | 返回 `*Run`；取消可能仍为 `CANCELLING` |
| `engine.ListRuns(ctx, limit, offset)` | 返回 `[]Run`，最新在前；limit 1–500、offset 非负 |
| `engine.GetRequest(ctx, id)` / `engine.CancelRequest(ctx, id)` | 返回 `*RequestStatus`，`Run` 可为 nil |
| `engine.ListBatches(ctx, runID)` | 返回 `[]Batch`，不包含表数据 |
| `engine.WaitForCompletion(ctx, id, ...WaitOption)` | 默认 300 秒、0.5 秒轮询 |
| `WithWaitTimeout(duration)` / `WithPollInterval(duration)` | 配置本地等待期限与查询间隔 |
| `engine.StateStoreID()` | 缓存的状态库身份 string |
| `engine.Close(ctx)` | 禁止新工作，取消活动作业并等待释放资源 |
| `Connectors(ctx)` / `RequestSchema(ctx)` | Rust 生成的能力描述和 JSON Schema，不打开状态库 |
| `Version` | 包与内嵌引擎版本常量 |

计数使用 `uint64`；配置和 schema 的自由结构使用 `json.RawMessage`，避免把大整数转成 `float64`。运行 `Config` 是脱敏审计记录，不能直接作为有效配置重放。配置中的 i64/u64/Decimal 参数值和分片整数边界保持 JSON 字符串；二进制参数保持 Base64。

API 错误可用 `errors.As` 读取 `*dunnelean.Error` 的 `Code`、`Message`、`CommitUnknown` 和 `Retryable`。等待期限耗尽为 `*WaitTimeoutError`，提供 `RunID` 和 `Timeout`；关闭超时为 `*CloseTimeoutError`。两种超时支持 `errors.Is(err, context.DeadlineExceeded)`。`Retryable` 描述对应操作，不授权重放整个同步作业。

一个状态库同一时间只允许一个拥有者，包括 Go/Python 实例和 HTTP 服务。锁冲突报 `STATE_STORE`；应用应给不同实例使用不同持久文件。成功 `Close` 等待任务及进行中的调用退出，随后释放状态库锁；立即重开同一路径有效。重复关闭安全返回。

关闭超时保留 runtime/Store 和文件锁，仍允许查询、取消和再次关闭；提交与验证报 `ENGINE_CLOSING`。关闭完成后其他实例方法报 `ENGINE_CLOSED`。取消 `Close` 的 context 不会撤销已经发出的关闭；原生关闭仍按本次预算继续收尾，再次 `Close` 可确认结果。

```go
if err := engine.Close(shortCtx); err != nil {
    // context 已结束或原生关闭超时，实例可能仍在收尾。
    _, _ = engine.ListRuns(context.Background(), 50, 0)
    err = engine.Close(context.Background())
    _ = err
}
```

请显式关闭。GC 的后台清理仅作兜底，进程终止时无法保证收尾；下一次打开状态库沿用 `INTERRUPTED` 审计语义，不自动恢复数据传输。实例不得跨 fork 使用。

## 示例、验收和维护者发布

仓库的 `examples/go` 是独立消费者 module，按正式版本导入 SDK。安装 Go 后在该目录运行：

```sh
go run ./offline
go run ./sync -spec ../mysql-to-doris.json
go run ./sync -spec ../doris-to-mysql.json -state var/go-return.sqlite
go run ./sync -spec ../mysql-to-doris.json -validate-only
```

`offline` 无需数据库，验证内嵌引擎打开、取消记录、关闭与重开；`sync` 需要可连接的数据库和密码环境变量，打印 request ID、状态库身份及完整运行。`-request-id` 可指定业务请求 ID，省略时为本次执行生成新 ID。示例不打印原始配置密码。

[Go CI](https://github.com/casperfrome/Dunnelean/blob/main/.github/workflows/go.yml) 构建四平台原生库，在 Go 1.25/1.26/1.27 上从标准 module ZIP 安装到全新仓库外项目，运行自包含测试和 `go vet`，然后 vendor、清空构建缓存，在 `GOPROXY=off` 和 `CGO_ENABLED=0` 下重建并运行离线示例。Linux 单独用 C 编译器运行 Go race detector；这不意味着应用需要 C 编译器，也不检测 Rust 内存竞争。

真实 MySQL/Doris 验收使用独立测试库、临时唯一表以及行级核对。报告通过 `scripts/integration-go.py` 生成，覆盖双向同步、取消、COMMIT 回执丢失、计数与不重放。CI 使用本地模拟端点，并不声明完成真实数据库验收。

维护者才需要 Python、Rust 和本机编译工具。Windows 本地 Python 命令统一使用 `D:\PythonVenv\Scripts\python.exe`；32 位 Go 不能作为首版执行验收，需要独立的 64 位 Go 安装。构建示例：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' scripts/build-go-native.py build --target x86_64-pc-windows-msvc --out dist/go-native
& 'D:\PythonVenv\Scripts\python.exe' scripts/build-go-native.py assemble --artifacts dist/go-native --sdk sdk/go
& 'D:\PythonVenv\Scripts\python.exe' scripts/build-go-native.py verify --sdk sdk/go
& 'D:\PythonVenv\Scripts\python.exe' scripts/verify-go-package.py --go '<64-bit Go 的 go.exe 路径>' --report var/acceptance-go-installed.json
```

`assemble` 需要同一源码提交构建的全部四平台资产，可从 CI 的 `go-native-*` artifacts 收集。manifest 保存源码提交、规范化 Rust 源码哈希、ABI/版本、目标与依赖、原始和压缩长度及 SHA-256。后续源码变化时必须重建资产；CI 检查已提交的 manifest，不会静默用临时构建覆盖过期库。

发布顺序固定为：提交实现 → 对该提交构建四平台资产 → 校验并提交 SDK 内的资产与 manifest → 对实际提交资产完成全部安装、离线及数据库验收 → 创建不可变 tag `sdk/go/v0.1.0` → 仓库外、空 module cache 验证真实 `go get @v0.1.0`。首轮 CI 缺少已提交资产时，只为测试组装 CI staging 资产；正式 tag 必须包含全部资产。压缩库以普通 Git 文件存储，不使用 LFS 或消费者安装脚本。

正式 tag 后可验证公共安装：

```powershell
& 'D:\PythonVenv\Scripts\python.exe' scripts/verify-go-package.py --go '<64-bit Go 的 go.exe 路径>' --published --version v0.1.0 --report var/acceptance-go-published.json
```

更新版本时同步 Go `Version`、Rust native crate 版本、Go 示例依赖、文档与资产 manifest；ABI 不兼容变化必须提升 ABI 版本并修改两侧绑定。Go 子模块 tag 使用 `sdk/go/v...` 前缀，不移动或覆盖已发布 tag。[Go module 发布规则](https://go.dev/ref/mod#vcs-version)
