package io.github.casperfrome.dunnelean.example;

import io.github.casperfrome.dunnelean.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** A separate consumer project: it depends only on the installed SDK artifact. */
public final class SdkExample {
    private SdkExample() { }

    public static void main(String[] args) throws Exception {
        List<String> options = Arrays.asList(args);
        RunSpec original = args.length == 0 || args[0].startsWith("--")
                ? typedMysqlToDoris() : RunSpecJson.read(Path.of(args[0]));
        if (options.contains("--check")) {
            RunSpec roundTrip = RunSpecJson.fromJson(RunSpecJson.toJson(original));
            if (!roundTrip.equals(original)) throw new IllegalStateException("Configuration round-trip failed");
            System.out.println("SDK dependency and typed configuration OK: " + original.reader().getClass().getSimpleName());
            return;
        }
        // Create a fresh ID once; reuse the resulting spec for every retry/query of this execution.
        RunSpec spec = original.requestId() == null
                ? original.toBuilder().requestId(UUID.randomUUID().toString()).build() : original;
        System.out.println("request_id=" + spec.requestId());
        String url = System.getenv().getOrDefault("DUNNELEAN_URL", "http://127.0.0.1:9876");
        DunneleanClient discovery = DunneleanClient.builder().baseUrl(url).build();
        Health health = discovery.health();
        System.out.println("state_store_id=" + health.stateStoreId());
        // A durable coordinator should reload its saved identity instead of rediscovering it.
        DunneleanClient client = DunneleanClient.builder().baseUrl(url).stateStoreId(health.stateStoreId()).build();
        try {
            Run submitted;
            if (options.contains("--async")) {
                submitted = client.validateAsync(spec).thenCompose(validation -> client.submitAsync(spec)).join();
            } else {
                client.validate(spec);
                submitted = client.submit(spec);
            }
            System.out.println("run_id=" + submitted.runId());
            if (options.contains("--deduplicate")) {
                Run duplicate = client.submit(spec);
                RequestStatus status = client.getRequest(spec.requestId());
                if (!submitted.runId().equals(duplicate.runId()) || status.run() == null
                        || !submitted.runId().equals(status.run().runId())) {
                    throw new IllegalStateException("Idempotency contract violated");
                }
                System.out.println("Duplicate request returned the same run_id");
            }
            if (options.contains("--cancel")) client.cancelRequest(spec.requestId());
            Run result = options.contains("--async")
                    ? client.waitForCompletionAsync(submitted.runId(), Duration.ofMinutes(5), Duration.ofMillis(200)).join()
                    : client.waitForCompletion(submitted.runId(), Duration.ofMinutes(5), Duration.ofMillis(200));
            System.out.println("state=" + result.state() + " rows_committed=" + result.rowsCommitted()
                    + " partial_write=" + result.partialWrite() + " commit_unknown=" + result.commitUnknown());
            if (!result.state().equals("SUCCEEDED") && !(options.contains("--cancel") && result.state().equals("CANCELLED"))) {
                throw new IllegalStateException("Run ended in " + result.state());
            }
        } catch (DunneleanException | java.util.concurrent.CompletionException e) {
            Throwable failure = e instanceof java.util.concurrent.CompletionException ? e.getCause() : e;
            if (failure instanceof DunneleanApiException api && api.error() != null) {
                System.err.println("HTTP " + api.statusCode() + " " + api.error().code()
                        + ": " + api.error().message() + " commit_unknown=" + api.error().commitUnknown()
                        + " retryable=" + api.error().retryable());
            }
            // A lost submit response must be reconciled by requestId; do not create a new ID here.
            throw e;
        }
    }

    /** The same MySQL → Doris job can be created entirely with Java types. */
    public static RunSpec typedMysqlToDoris() {
        Credentials credentials = Credentials.builder().username("dunnelean")
                .passwordEnv("DUNNELEAN_TEST_PASSWORD").build();
        MysqlConnection mysql = MysqlConnection.builder().host("127.0.0.1").port(3308)
                .database("dunnelean_test").credentials(credentials).build();
        MysqlConnection dorisSql = MysqlConnection.builder().host("127.0.0.1").port(9030)
                .database("dunnelean_test").credentials(credentials).build();
        return RunSpec.builder()
                .reader(MysqlReader.builder().connection(mysql)
                        .source(Source.builder().table("source_orders").build()).build())
                .writer(DorisWriter.builder().sql(dorisSql).feHttpUrls(List.of("http://127.0.0.1:8030"))
                        .beHttpUrls(List.of("http://127.0.0.1:8040")).table("target_orders")
                        .mode(DorisWriteMode.APPEND).build())
                .build();
    }
}
