# Java SDK

SDK 支持 Java 17+，封装 Dunnelean 的 HTTP 控制接口，提供不可变配置类型、builder、同步方法和 `CompletableFuture` 异步方法。作业数据仍由独立运行的 Rust 服务传输，Java 项目无需 Arrow、JDBC 或同步数据库驱动。

## 安装与依赖

在仓库根目录执行，要求 JDK 17+ 和 Maven：

```powershell
mvn -B -ntp -f sdk/java/pom.xml install
```

这会测试并将普通 JAR、源码 JAR、Javadoc JAR 和 POM 安装到本机 Maven 仓库。坐标尚未发布到 Maven Central；其他开发者需要先在自己的机器上执行安装。项目直接使用以下依赖，Jackson 由 Maven 自动解析：

```xml
<dependency>
  <groupId>io.github.casperfrome</groupId>
  <artifactId>dunnelean-java-sdk</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle：

```groovy
repositories {
    mavenLocal()
    mavenCentral()
}
dependencies {
    implementation 'io.github.casperfrome:dunnelean-java-sdk:0.1.0'
}
```

SDK 使用 JDK `HttpClient` 和 Jackson BOM `2.21.5`，没有 Spring 或 Lombok 运行时依赖。所有源文件及 JSON 文件以 UTF-8 读写，编译目标为 Java 17。

## 用类型构建并提交作业

先按 [README](../README.md) 启动服务并准备目标表。下面的环境变量及证书路径均由 **Dunnelean 服务进程**解析，Java 不会替服务读取它们。

```java
import io.github.casperfrome.dunnelean.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

var credentials = Credentials.builder()
    .username("dunnelean").passwordEnv("DUNNELEAN_TEST_PASSWORD").build();
var mysql = MysqlConnection.builder().host("127.0.0.1").port(3308)
    .database("dunnelean_test").credentials(credentials).build();
var dorisSql = MysqlConnection.builder().host("127.0.0.1").port(9030)
    .database("dunnelean_test").credentials(credentials).build();

var spec = RunSpec.builder().requestId(UUID.randomUUID().toString())
    .reader(MysqlReader.builder().connection(mysql)
        .source(Source.builder().table("source_orders").build()).build())
    .writer(DorisWriter.builder().sql(dorisSql)
        .feHttpUrls(List.of("http://127.0.0.1:8030"))
        .beHttpUrls(List.of("http://127.0.0.1:8040"))
        .table("target_orders").mode(DorisWriteMode.APPEND).build())
    .build();

var client = DunneleanClient.builder().baseUrl("http://127.0.0.1:9876").build();
client.validate(spec);
Run submitted = client.submit(spec);
Run result = client.waitForCompletion(submitted.runId(),
    Duration.ofMinutes(5), Duration.ofMillis(500));
System.out.println(result.state() + " rows=" + result.rowsCommitted());
// FAILED/CANCELLED/INTERRUPTED 也会返回完整 Run；按业务需要检查 error/partialWrite/commitUnknown。
```

反向同步将 Reader/Writer 换为下面的类型；endpointMap 按实际 Doris FE/BE 宣告的地址配置：

```java
var reverse = RunSpec.builder().requestId(UUID.randomUUID().toString())
    .reader(DorisReader.builder().flightUri("grpc://127.0.0.1:8070")
        .database("dunnelean_test").credentials(credentials)
        .source(Source.builder().table("target_orders").build()).build())
    .writer(MysqlWriter.builder().connection(mysql)
        .table("returned_orders").mode(MysqlWriteMode.INSERT).build())
    .build();
```

配置类型完整覆盖 [参数手册](parameters.md)，builder 的省略字段使用服务默认值。`toBuilder()` 可复制配置后修改。集合会被复制并保持不可变。结构、整数范围和凭据互斥关系由 SDK 检查，数据库权限、SQL、表结构及跨配置限制由服务的 `validate` 检查。

所有无符号 64 位数值及未收窄的大小字段使用 `BigInteger`，常用 builder 提供 `long` 重载；超过 `Long.MAX_VALUE` 时传入 `BigInteger`。`Parameter.i64/u64/decimal` 和分片边界在 JSON 中保留字符串，`Parameter.binary(byte[])` 编码为 Base64；`Parameter.nullValue()` 不发送 `value`。凭据的 `toString()` 隐藏密码，但 `RunSpecJson.toJson` 为提交目的会包含显式配置的真实密码。

## 原有 JSON 与异步调用

```java
import java.nio.file.Path;

RunSpec spec = RunSpecJson.read(Path.of("examples/doris-to-mysql.json"));
spec = spec.toBuilder().requestId("saved-business-execution-id").build();
String json = RunSpecJson.toJson(spec);
RunSpec same = RunSpecJson.fromJson(json);
```

配置解析拒绝未知字段、重复字段、错误标量类型、非可选字段的 `null` 和尾随 JSON 内容。可选字段允许省略或 `null`；输出省略可选空值。

```java
client.submitAsync(spec)
    .thenCompose(run -> client.waitForCompletionAsync(run.runId(),
        Duration.ofMinutes(5), Duration.ofMillis(500)))
    .thenAccept(run -> System.out.println(run.state()));
```

异步请求及等待不阻塞调用线程。Client 可跨线程复用；不创建需要调用方关闭的 SDK 线程池。可以注入已有 `HttpClient`，其连接超时、TLS 和代理配置由调用方负责；SDK 的请求超时仍生效。

## 接口与响应

以下方法均有同名加 `Async` 的异步版本。同步方法可抛出 `InterruptedException`，并保留线程中断标记。

| 方法 | 返回类型 |
| --- | --- |
| `health()` / `ready()` | `Health` / `Ready` |
| `connectors()` / `openApi()` | `Connectors` / `JsonNode` |
| `validate(spec)` / `submit(spec)` | `Validation` / `Run` |
| `listRuns()` / `listRuns(limit, offset)` | `RunList`，默认 50/0，limit 为 1–500 |
| `getRun(id)` / `cancelRun(id)` | `Run` |
| `listBatches(id)` | `BatchList` |
| `getRequest(requestId)` / `cancelRequest(requestId)` | `RequestStatus`，run 可为空 |
| `waitForCompletion(id, timeout, interval)` | `Run`，等待任意终态 |

返回对象使用 record 访问器，例如 `run.runId()`、`run.rowsCommitted()`、`status.run()`。时间为 `Instant`，计数为 `BigInteger`。`knownState()` 返回已知枚举的 `Optional`，`terminal()` 判断终态；未知运行状态保留原字符串并继续等待至期限。

响应允许新增字段。`Run.config` 是脱敏回显，连接器能力描述、JSON Schema 和 OpenAPI 保留为 `JsonNode`；不要将脱敏配置重新提交。批次 `detail` 保持原字符串，它可能是数据库凭据或错误文本。

## 错误与可靠控制

- `DunneleanApiException`：HTTP 非 2xx。保留 `statusCode()`、`responseBody()` 和可空的 `error()`；结构化错误有 code、message、commitUnknown、retryable。非标准 HTTP 错误也保留状态和正文，诊断正文最多保存 64 Ki 字符。
- `DunneleanTransportException`：连接失败、HTTP 请求超时等网络错误，原异常保存在 cause。
- `DunneleanProtocolException`：成功响应无法解析为预期 JSON 类型。
- `DunneleanTimeoutException`：轮询等待期限耗尽，保留 runId 和 timeout。

上述异常继承 `DunneleanException`。异步失败由 Future 传递；使用 `join()` 时通过 `CompletionException.getCause()` 取得 SDK 异常。

协调器首次执行时获取并持久保存状态库身份，后续会话使用保存值：

```java
String savedStoreId = client.health().stateStoreId(); // 与本地执行记录一起保存
var guarded = DunneleanClient.builder().stateStoreId(savedStoreId).build();
RequestStatus status = guarded.getRequest(spec.requestId());
guarded.cancelRequest(spec.requestId());
```

Client 默认连接超时 10 秒、请求超时 3 分钟。等待期间单次请求还受剩余等待期限限制。超时、线程中断或取消等待 Future 只结束本地等待；远程停止必须显式调用取消接口。取消不撤销已提交批次，返回 `CANCELLING` 时仍需查询终态。

SDK 不主动重试提交，也不会在 `STATE_STORE_CHANGED` 后刷新身份。同一 requestId 和同一完整配置只能找回原运行，重跑使用新 ID。提交响应丢失时先查询 requestId；同一状态库确认没有记录后才使用完全相同配置与 ID 再提交。`retryable` 不代表可以重放整个业务作业。详细语义见 [可靠控制协议](studio-control.md)。

## 独立示例与验证

仓库根目录执行：

```powershell
mvn -B -ntp -f examples/java/sdk/pom.xml package
$sdkClasspath = 'examples/java/sdk/target/classes;examples/java/sdk/target/dependency/*'
$sdkMain = 'io.github.casperfrome.dunnelean.example.SdkExample'
# 仅验证独立项目依赖和配置，不访问服务：
java -cp $sdkClasspath $sdkMain examples/mysql-to-doris.json --check
java -cp $sdkClasspath $sdkMain examples/doris-to-mysql.json --check
# 访问已准备数据库和示例表的服务：
java -cp $sdkClasspath $sdkMain examples/mysql-to-doris.json --deduplicate
java -cp $sdkClasspath $sdkMain examples/doris-to-mysql.json --async
java -cp $sdkClasspath $sdkMain examples/mysql-to-doris.json --cancel
```

不传文件时示例使用 Java builder 创建 MySQL → Doris 配置；服务地址可通过 `DUNNELEAN_URL` 指定。Linux/macOS 的 classpath 分隔符改为 `:`。

默认 `mvn verify` 使用本地 HTTP mock，无需 MySQL/Doris。真实 Rust 控制接口测试使用临时状态库、独立端口和子进程，也无需数据库：

```powershell
cargo build --release --locked
mvn -B -ntp -f sdk/java/pom.xml -Prust-api-it verify
```

Linux 或其他二进制路径通过 `-Ddunnelean.binary=/absolute/path/to/dunnelean` 指定。独立数据库的完整 SDK 双向同步、去重、异步和取消检查已加入 `scripts/integration-more.py`，运行前需按 README 完成基础数据库验收及 fixtures 初始化。

### 本次验证记录（2026-09-30）

- Windows 11，JDK 25.0.2，Maven 3.9.12；`release=17`，生成的 class major version 为 61。未单独运行 JDK 17。
- 42 项 Java 单元测试及 1 项真实 Rust 控制接口集成测试全部通过；本地安装普通、源码、Javadoc 三个 JAR 成功。
- 独立消费项目通过编译，两个现有 JSON 配置和纯 builder 配置均完成解析/序列化运行检查。
- Rust 格式、Clippy 与 17 项测试通过；新增双向完整配置契约测试与 Java 输出使用同一份 fixture。
- 当前 Docker Desktop Linux Engine 未运行，本次未执行数据库端到端验收；新增 SDK 数据库验收入口及 Python 语法检查已完成。
