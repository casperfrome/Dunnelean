# Go v0.1.0 验收记录

2026-10-02 完成发布验收。原生库源提交为 `53ae345a452aac583ec4a524a09cc3a8f0e49d53`，ABI 为 1。四平台资产以普通 Git 文件保存，Go module 大小为 30,648,872 字节，低于 500 MiB 的 ZIP 上限。

[源构建 CI](https://github.com/casperfrome/Dunnelean/actions/runs/36971677790) 完成四平台构建；[实际提交资产 CI](https://github.com/casperfrome/Dunnelean/actions/runs/36973613114) 的 17 项检查全部通过，包括四平台原生依赖检查、12 组实际 module 安装及离线构建，以及 Linux Go race 检查。

| 平台 | 实际验证 Go 版本 | 安装、测试、vet、空缓存离线构建 |
| --- | --- | --- |
| `windows_amd64` | go1.25.14, go1.26.8, go1.27.1 | 通过 |
| `linux_amd64` | go1.25.14, go1.26.8, go1.27.1 | 通过 |
| `darwin_amd64` | go1.25.14, go1.26.7, go1.27.1 | 通过 |
| `darwin_arm64` | go1.25.14, go1.26.8, go1.27.1 | 通过 |

每次安装使用仓库外临时项目、全新 module/build cache；安装后 vendor 依赖，再在 `GOPROXY=off`、空 build/module cache 和 `CGO_ENABLED=0` 下构建可执行程序。程序使用全新的原生缓存，成功打开 Rust 引擎、创建取消墓碑、关闭和重开持久状态库。Linux race 使用维护者 C 编译器，普通消费者构建不需要 C 编译器。

实际 CI Windows 原生库在独立 MySQL/Doris 测试库完成五项验收：

| 场景 | 作业状态 | 已确认提交行数 | 验证结果 |
| --- | --- | --- | --- |
| MySQL → Doris | SUCCEEDED | 47 | 整数、Decimal、NULL、Unicode、微秒时间逐行相同 |
| goroutine 中 Doris → MySQL | SUCCEEDED | 47 | 逐行往返相同，其他 goroutine 持续响应 |
| 丢失 MySQL COMMIT 回执 | FAILED | 0 | 目标实际有 2 行，CommitUnknown=true，同 request 不重放，COMMIT 仅 1 次 |
| context 结束后显式取消 | CANCELLED | 0 | 本地等待结束没有隐式取消作业 |
| Doris 已提交、回执延迟，关闭超时后重试 | CANCELLED | 2 | 资源保留，仍可查询/取消，释放回执后重试关闭成功，无第二次提交 |

SDK 自包含测试覆盖配置兼容和未知字段、幂等冲突/墓碑、跨进程状态库独占、进程中断审计、不重放、关闭重试及竞态、context 取消后的后台清理、GC、环境变量传播、缓存并发与完整性、ABI/版本错误和完整 uint64 精度。Windows 的 Unix 权限测试按平台跳过；Unix CI 执行该测试。Rust C ABI 测试确认 panic 转为结构化错误。

既有回归：Rust 33 项测试、format 和 Clippy 全部通过；实际 Python wheel 31 项测试、仓库外 sdist 重建 wheel 后再次 31 项测试通过，四平台/Python 3.10–3.14 的 24 个 CI jobs 全部通过；Python 五项真实数据库验收通过；Java 42 项单元测试与 1 项真实 Rust HTTP 集成通过。

详细记录：[Go CI](acceptance-go-ci.json)、[Go 数据库及安装](acceptance-go.json)、[Rust/Python/Java 回归](acceptance-go-regressions.json)。

不可变标签 `sdk/go/v0.1.0` 指向 `c4e0ab039aeb1d8ae0d98af3fe44b5db85f46c15`，已创建 [GitHub Release](https://github.com/casperfrome/Dunnelean/releases/tag/sdk/go/v0.1.0)。[正式标签公开安装 CI](https://github.com/casperfrome/Dunnelean/actions/runs/36974394473) 的 17 项检查全部通过；12 组安装与 Linux race 均从公共分发执行真实 `go get @v0.1.0`，报告全部 `published:true`。

本机也从仓库外、空 module cache 安装公共版本，无本地 replace，完整测试、vet、全新原生缓存、空缓存离线构建通过；用同一公共版本再次执行上述五项真实数据库验收，全部通过。原有 32 位 Go 安装未被覆盖，本机验收使用独立的官方 Go 1.27.1 windows/amd64。
