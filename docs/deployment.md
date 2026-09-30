# 本机部署

Dunnelean 在 Windows 上作为原生进程独立运行。本文的可选本地测试拓扑使用 Docker Desktop Linux 容器运行 Doris FE/BE 4.1.4 存算一体及测试 MySQL 9.7.2。

## 独立启动 Dunnelean

在项目根目录执行：

```powershell
cargo build --release --locked # 首次运行或更新代码后构建
.\scripts\run.ps1
```

`run.ps1` 只启动 Dunnelean，不调用 Docker，也不会启动数据库容器。默认监听 `http://127.0.0.1:9876`，状态保存在 `var/dunnelean.sqlite`。服务启动不要求数据库可连接；验证或执行同步任务时，相关数据库需要可连接。

接入已有 MySQL/Doris 时，修改作业的连接、来源和目标配置，预先创建目标表，并在启动服务前设置 `password_env` 指定的环境变量。`run.ps1` 会在 `deploy/.env` 存在时加载其中的 `DUNNELEAN_TEST_PASSWORD`。

保持服务终端运行，在另一个 PowerShell 窗口检查服务及状态库：

```powershell
Invoke-RestMethod http://127.0.0.1:9876/healthz
Invoke-RestMethod http://127.0.0.1:9876/readyz
```

成功时分别返回 `status: ok` 和 `status: ready`。这两个接口不校验数据库连接，作业配置需通过 `/v1/validate` 检查。本项目已在 Windows MSVC 上验收；Linux 编译尚未完成实测。

## 可选：本地测试数据库

需要本项目自带的测试数据库时，在项目根目录准备 Python 环境，再初始化数据库。以下命令中的 `python` 应指向本机 Python 3 解释器；已验收版本为 3.10.11。若已按 [README](../README.md) 创建 `.venv` 并安装依赖，可跳过前两条命令。

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install pymysql requests psutil
.\scripts\start-env.ps1
.\.venv\Scripts\python.exe scripts/init-test-env.py
```

`start-env.ps1` 启动 Doris FE/BE 和测试 MySQL，并在首次运行时将随机开发密码保存在忽略文件 `deploy/.env`；`init-test-env.py` 初始化测试 Doris 数据库和业务账号。完成后启动或重启 Dunnelean，使服务加载测试密码。以下容器、资源和网络配置均针对该测试环境。

初始化脚本不会创建 `examples/*.json` 引用的 `source_orders`、`target_orders`、`returned_orders`。在该独立测试库运行 README 中的基础验收命令可创建示例表并验证双向同步；接入已有数据库时，需要自行准备目标表并调整作业配置。

## 容器与地址

| 服务 | 内部 IP | 宿主回环端口 | 资源 |
|---|---|---|---|
| FE | 172.30.41.2 | 9030 SQL、8030 HTTP、8070 Flight | 2 CPU、3 GiB，JVM 堆 2 GiB |
| BE | 172.30.41.3 | 8040 HTTP、8050 Flight | 4 CPU、6 GiB，mem_limit=4G，JNI 堆 1 GiB |
| 测试 MySQL（test profile） | 172.30.41.4 | 3308 | 768 MiB、InnoDB 缓冲 128 MiB |

项目名 `dunnelean`，子网 `172.30.41.0/24`，镜像 digest 已锁定。测试 MySQL 也固定内部地址，避免 Docker 自动分配地址与尚未启动的 BE 冲突。

FE/BE 各有数据/元数据、配置、日志三个 named volumes。配置卷首次使用镜像内容填充；configure.sh 不覆盖整个 conf 目录，重复启动只更新指定参数。`stop-env.ps1` 只停止容器；常规 down 也保留 named volumes，不要添加 `-v`。

## Docker Desktop 兼容性

测试环境启动脚本 `start-env.ps1` 检查 Docker 内存至少 11 GiB、CPU 至少 6 核、可用磁盘至少 8 GiB、`vm.max_map_count >= 2000000`。这些检查仅用于启动测试容器，独立启动 Dunnelean 不依赖 Docker。资源不足时报告检查项，由宿主 Docker/WSL 配置调整。Compose 设置容器日志轮转、nofile=1000000、120 秒停止宽限。

该镜像内 JDK 在本机 cgroup v2 环境会触发 `CgroupV2Subsystem` 初始化空指针。configure.sh 设置 `-XX:-UseContainerSupport` 并显式指定 JVM 堆和处理器数；Docker 容器的 OS 内存/CPU 限制仍生效。这些 JVM 参数幂等更新，不会重复追加。

FE 的健康检查使用 `SHOW FRONTENDS`。初次启动时 `SELECT 1` 在此版本也可能需要 BE，若以此作为 BE 的依赖条件会形成启动阻塞。BE 健康检查确认其已注册并 Alive=true。

## Flight 和 Stream Load

BE 配置 `public_host=127.0.0.1`、`arrow_flight_sql_proxy_port=8050`，因此返回给 Windows 客户端的 BE Flight 地址可达。

FE 的 USE、SET、空查询等结果仍可能发布 Docker 内部 FE 地址，所以示例 Reader 同时提供：

```json
"endpoint_map": {
  "grpc+tcp://172.30.41.2:8070": "grpc://127.0.0.1:8070",
  "grpc+tcp://172.30.41.3:8050": "grpc://127.0.0.1:8050"
}
```

本机 Writer 直接向 `http://127.0.0.1:8040` 导入，FE `8030` 用于核实 label 状态。接入其他集群时可以使用 FE 307 重定向，并配置 `endpoint_map` 把内部 HTTP origin 映射到可达地址；PUT、认证、label 和请求体都保留。

这些回环地址只适合客户端运行于宿主机。把 Dunnelean 移入容器时，必须改为客户端可达的容器地址，不能照抄 127.0.0.1。多 BE 集群需要为每个发布地址提供独立可达映射。

## 账号与数据

FE 保留官方入口注册 BE 所需的本地 bootstrap 行为，宿主端口仅绑定回环。`init-test-env.py` 创建独立 `dunnelean_test` 库及有密码业务账号，仅授予该库的读取、导入和测试 DDL 权限。应用配置使用业务账号。密码不进入提交文件和 API 回显。

测试表显式设 `replication_num=1`，不修改集群全局副本策略。现有 `3306/3307` MySQL 容器不受本项目脚本管理。

## 检查与重建

```powershell
docker compose --env-file deploy/.env -f deploy/compose.yml --profile test ps
docker compose --env-file deploy/.env -f deploy/compose.yml logs --tail 50 fe be
# 重建容器保留数据卷：
docker compose --env-file deploy/.env -f deploy/compose.yml --profile test up -d --force-recreate --wait
.\scripts\stop-env.ps1
```

更换镜像版本前备份数据与配置卷；本项目不会自动执行升级或删除卷。首次调试失败留下的 FE 元数据副本位于该卷的 `failed-bootstrap-20260929`，正常服务不读取该目录。
