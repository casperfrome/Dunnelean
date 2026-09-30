package io.github.casperfrome.dunnelean;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DunneleanClientTest {
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private static final String SPEC = """
            {
              "request_id":"request-fixture",
              "reader":{"type":"mysql","connection":{"database":"source",
                "credentials":{"username":"test","password":""}},"source":{"table":"orders"}},
              "writer":{"type":"doris","sql":{"database":"target",
                "credentials":{"username":"test","password":""}},
                "fe_http_urls":["http://127.0.0.1:8030"],"table":"orders"}
            }
            """;
    private static final String ERROR = """
            {"error":{"code":"COMMIT_UNKNOWN","message":"响应丢失",
              "commit_unknown":true,"retryable":false}}
            """;

    private HttpServer server;
    private ExecutorService executor;
    private String base;
    private DunneleanClient client;
    private final BlockingQueue<RecordedRequest> requests = new LinkedBlockingQueue<>();
    private final AtomicReference<Responder> responder = new AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool(task -> {
            var thread = new Thread(task, "dunnelean-sdk-test-http");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        responder.set(this::standardResponse);
        server.createContext("/", exchange -> {
            var request = new RecordedRequest(
                    exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
                    exchange.getRequestURI().getRawQuery(),
                    Map.copyOf(exchange.getRequestHeaders()),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            requests.add(request);
            responder.get().respond(exchange, request);
        });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
        client = DunneleanClient.builder().baseUrl(base).requestTimeout(Duration.ofSeconds(2)).build();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        executor.shutdownNow();
    }

    @Test
    void everySynchronousOperationUsesTheTypedContract() throws Exception {
        assertOperations(false);
    }

    @Test
    void everyAsynchronousOperationUsesTheTypedContract() throws Exception {
        assertOperations(true);
    }

    private void assertOperations(boolean async) throws Exception {
        var health = async ? await(client.healthAsync()) : client.health();
        assertEquals("ok", health.status());
        assertEquals("store-fixture", health.stateStoreId());
        assertRequest("GET", "/healthz", null, false);
        var ready = async ? await(client.readyAsync()) : client.ready();
        assertEquals("ready", ready.status());
        assertRequest("GET", "/readyz", null, false);
        var connectors = async ? await(client.connectorsAsync()) : client.connectors();
        assertEquals("mysql", connectors.connectors().get(0).path("type").asText());
        assertTrue(connectors.requestSchema().path("future_keyword").asBoolean());
        assertRequest("GET", "/v1/connectors", null, false);
        var openapi = async ? await(client.openApiAsync()) : client.openApi();
        assertEquals("3.1.0", openapi.path("openapi").asText());
        assertRequest("GET", "/openapi.json", null, false);

        var spec = RunSpecJson.fromJson(SPEC);
        var validation = async ? await(client.validateAsync(spec)) : client.validate(spec);
        assertTrue(validation.valid());
        assertEquals("UInt64", validation.sourceSchema().get(0).type());
        assertEquals("batch commits", validation.semantics());
        assertRequest("POST", "/v1/validate", null, true);
        var submitted = async ? await(client.submitAsync(spec)) : client.submit(spec);
        assertRun(submitted);
        assertRequest("POST", "/v1/runs", null, true);

        var defaultPage = async ? await(client.listRunsAsync()) : client.listRuns();
        assertEquals(1, defaultPage.runs().size());
        assertRequest("GET", "/v1/runs", "limit=50&offset=0", false);
        var page = async ? await(client.listRunsAsync(123, 4_000_000_000L))
                : client.listRuns(123, 4_000_000_000L);
        assertRun(page.runs().get(0));
        assertRequest("GET", "/v1/runs", "limit=123&offset=4000000000", false);

        assertRun(async ? await(client.getRunAsync("run-fixture")) : client.getRun("run-fixture"));
        assertRequest("GET", "/v1/runs/run-fixture", null, false);
        assertRun(async ? await(client.cancelRunAsync("run-fixture")) : client.cancelRun("run-fixture"));
        assertRequest("POST", "/v1/runs/run-fixture/cancel", null, false);
        var batches = async ? await(client.listBatchesAsync("run-fixture"))
                : client.listBatches("run-fixture");
        assertEquals(UINT64_MAX, batches.batches().get(0).batchId());
        assertEquals(UINT64_MAX, batches.batches().get(0).rows());
        assertNull(batches.batches().get(0).label());
        assertEquals("opaque diagnostic", batches.batches().get(0).detail());
        assertRequest("GET", "/v1/runs/run-fixture/batches", null, false);

        var request = async ? await(client.getRequestAsync("request-fixture"))
                : client.getRequest("request-fixture");
        assertFalse(request.cancelRequested());
        assertRun(request.run());
        assertRequest("GET", "/v1/requests/request-fixture", null, false);
        var cancelled = async ? await(client.cancelRequestAsync("request-fixture"))
                : client.cancelRequest("request-fixture");
        assertTrue(cancelled.cancelRequested());
        assertNull(cancelled.run());
        assertRequest("POST", "/v1/requests/request-fixture/cancel", null, false);
        assertTrue(requests.isEmpty());
    }

    @Test
    void idsAreUtf8EncodedAsSinglePathSegmentsInBothStyles() throws Exception {
        String id = "job-中文 /?#%+";
        String encoded = "job-%E4%B8%AD%E6%96%87%20%2F%3F%23%25%2B";
        for (boolean async : List.of(false, true)) {
            if (async) {
                await(client.getRunAsync(id));
                await(client.cancelRunAsync(id));
                await(client.listBatchesAsync(id));
                await(client.getRequestAsync(id));
                await(client.cancelRequestAsync(id));
            } else {
                client.getRun(id);
                client.cancelRun(id);
                client.listBatches(id);
                client.getRequest(id);
                client.cancelRequest(id);
            }
            assertRequest("GET", "/v1/runs/" + encoded, null, false);
            assertRequest("POST", "/v1/runs/" + encoded + "/cancel", null, false);
            assertRequest("GET", "/v1/runs/" + encoded + "/batches", null, false);
            assertRequest("GET", "/v1/requests/" + encoded, null, false);
            assertRequest("POST", "/v1/requests/" + encoded + "/cancel", null, false);
        }
    }

    @Test
    void storeIdentityIsOptionalAndSuppliedHttpClientIsSupported() throws Exception {
        client.health();
        assertNull(takeRequest().header("x-dunnelean-state-store-id"));
        var guarded = DunneleanClient.builder().baseUrl(java.net.URI.create(base + "/"))
                .httpClient(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build())
                .connectTimeout(Duration.ofSeconds(1)).requestTimeout(Duration.ofSeconds(1))
                .stateStoreId("expected-store").build();
        guarded.health();
        assertEquals("expected-store", takeRequest().header("x-dunnelean-state-store-id"));
        await(guarded.cancelRequestAsync("request-fixture"));
        assertEquals("expected-store", takeRequest().header("x-dunnelean-state-store-id"));
    }

    @Test
    void structuredApiErrorsRetainStatusAndCommitInformation() throws Exception {
        responder.set((exchange, request) -> reply(exchange, 409, ERROR));
        var synchronous = assertThrows(DunneleanApiException.class,
                () -> client.submit(RunSpecJson.fromJson(SPEC)));
        assertApiError(synchronous);
        var asynchronous = assertInstanceOf(DunneleanApiException.class,
                failed(client.getRequestAsync("request-fixture")));
        assertApiError(asynchronous);
    }

    @Test
    void nonJsonErrorsAndInvalidSuccessBodiesRemainDiagnosable() throws Exception {
        responder.set((exchange, request) -> reply(exchange, 502, "gateway unavailable"));
        var gateway = assertThrows(DunneleanApiException.class, client::health);
        assertEquals(502, gateway.statusCode());
        assertNull(gateway.error());
        assertEquals("gateway unavailable", gateway.responseBody());
        var gatewayAsync = assertInstanceOf(DunneleanApiException.class, failed(client.healthAsync()));
        assertEquals("gateway unavailable", gatewayAsync.responseBody());

        responder.set((exchange, request) -> reply(exchange, 200, "{broken-json"));
        var badJson = assertThrows(DunneleanProtocolException.class, client::health);
        assertEquals(200, badJson.statusCode());
        assertEquals("{broken-json", badJson.responseBody());
        assertInstanceOf(DunneleanProtocolException.class, failed(client.healthAsync()));

        responder.set((exchange, request) -> reply(exchange, 200, "{}"));
        assertThrows(DunneleanProtocolException.class, () -> client.getRun("missing-fields"));
    }

    @Test
    void submitDoesNotReplayServerFailures() throws Exception {
        var sends = new AtomicInteger();
        responder.set((exchange, request) -> {
            sends.incrementAndGet();
            reply(exchange, 500, ERROR);
        });
        assertThrows(DunneleanApiException.class, () -> client.submit(RunSpecJson.fromJson(SPEC)));
        assertEquals(1, sends.get());
        assertInstanceOf(DunneleanApiException.class, failed(client.submitAsync(RunSpecJson.fromJson(SPEC))));
        assertEquals(2, sends.get());
    }

    @Test
    void submitDoesNotReplayLostResponses() throws Exception {
        var sends = new AtomicInteger();
        responder.set((exchange, request) -> {
            sends.incrementAndGet();
            exchange.close();
        });
        assertThrows(DunneleanTransportException.class, () -> client.submit(RunSpecJson.fromJson(SPEC)));
        assertEquals(1, sends.get());
        assertInstanceOf(DunneleanTransportException.class,
                failed(client.submitAsync(RunSpecJson.fromJson(SPEC))));
        assertEquals(2, sends.get());
    }

    @Test
    void pollingPreservesFailedRunsAndContinuesPastUnknownStates() throws Exception {
        for (boolean async : List.of(false, true)) {
            var polls = new AtomicInteger();
            responder.set((exchange, request) -> reply(exchange, 200,
                    runJson(polls.incrementAndGet() == 1 ? "FUTURE_RUNNING_STATE" : "FAILED")));
            Run result = async
                    ? await(client.waitForCompletionAsync("poll-fixture", Duration.ofSeconds(2), Duration.ofMillis(10)))
                    : client.waitForCompletion("poll-fixture", Duration.ofSeconds(2), Duration.ofMillis(10));
            assertEquals("FAILED", result.state());
            assertTrue(result.terminal());
            assertTrue(result.partialWrite());
            assertTrue(result.commitUnknown());
            assertEquals("COMMIT_UNKNOWN", result.error().code());
            assertEquals(2, polls.get());
            assertEquals("/v1/runs/poll-fixture", takeRequest().path());
            assertEquals("/v1/runs/poll-fixture", takeRequest().path());
            assertTrue(requests.isEmpty());
        }
    }

    @Test
    void pollingHasABoundedDeadlineWithoutRemoteCancellation() throws Exception {
        responder.set((exchange, request) -> reply(exchange, 200, runJson("RUNNING")));
        Duration timeout = Duration.ofMillis(150);
        var sync = assertThrows(DunneleanTimeoutException.class,
                () -> client.waitForCompletion("deadline", timeout, Duration.ofMillis(20)));
        assertEquals("deadline", sync.runId());
        var async = assertInstanceOf(DunneleanTimeoutException.class,
                failed(client.waitForCompletionAsync("deadline", timeout, Duration.ofMillis(20))));
        assertEquals("deadline", async.runId());
        assertFalse(requests.isEmpty());
        assertTrue(requests.stream().allMatch(request -> request.method().equals("GET")
                && request.path().equals("/v1/runs/deadline")));
    }

    @Test
    void deadlineAlsoBoundsASlowHttpPoll() throws Exception {
        var release = new CountDownLatch(1);
        responder.set((exchange, request) -> {
            awaitRelease(release);
            reply(exchange, 200, runJson("RUNNING"));
        });
        try {
            Duration timeout = Duration.ofMillis(100);
            long started = System.nanoTime();
            assertThrows(DunneleanTimeoutException.class,
                    () -> client.waitForCompletion("slow", timeout, Duration.ofMillis(10)));
            assertTrue(Duration.ofNanos(System.nanoTime() - started).compareTo(Duration.ofSeconds(1)) < 0);
            assertInstanceOf(DunneleanTimeoutException.class,
                    failed(client.waitForCompletionAsync("slow", timeout, Duration.ofMillis(10))));
        } finally {
            release.countDown();
        }
    }

    @Test
    void blockedUserTimeoutCallbackDoesNotBlockOtherPollingDeadlines() throws Exception {
        responder.set((exchange, request) -> reply(exchange, 200, runJson("RUNNING")));
        var callbackEntered = new CountDownLatch(1);
        var releaseCallback = new CountDownLatch(1);
        var first = client.waitForCompletionAsync("first-deadline", Duration.ofMillis(150), Duration.ofSeconds(5));
        var heldCallback = first.exceptionally(failure -> {
            callbackEntered.countDown();
            try {
                assertTrue(releaseCallback.await(3, TimeUnit.SECONDS), "user callback was not released");
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new AssertionError(error);
            }
            return null;
        });
        CompletableFuture<Run> second = null;
        try {
            assertEquals("/v1/runs/first-deadline", takeRequest().path());
            assertTrue(callbackEntered.await(2, TimeUnit.SECONDS), "first deadline callback did not start");
            second = client.waitForCompletionAsync("second-deadline", Duration.ofMillis(150), Duration.ofSeconds(5));
            assertEquals("/v1/runs/second-deadline", takeRequest().path());
            CompletableFuture<Run> independentlyTimed = second;
            var failure = assertThrows(ExecutionException.class,
                    () -> independentlyTimed.get(1, TimeUnit.SECONDS));
            assertInstanceOf(DunneleanTimeoutException.class, failure.getCause());
            assertEquals(1, releaseCallback.getCount(), "first callback must still be blocked");
            assertTrue(requests.isEmpty(), "the second deadline must expire between HTTP polls");
        } finally {
            releaseCallback.countDown();
            if (second != null) second.cancel(true);
            first.cancel(true);
            heldCallback.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void shorterHttpRequestTimeoutRemainsATransportErrorInsidePolling() throws Exception {
        var release = new CountDownLatch(1);
        responder.set((exchange, request) -> {
            awaitRelease(release);
            reply(exchange, 200, runJson("RUNNING"));
        });
        var shortRequests = DunneleanClient.builder().baseUrl(base)
                .requestTimeout(Duration.ofMillis(100)).build();
        try {
            var sync = assertThrows(DunneleanTransportException.class,
                    () -> shortRequests.waitForCompletion("request-timeout", Duration.ofSeconds(2), Duration.ofMillis(10)));
            assertInstanceOf(java.net.http.HttpTimeoutException.class, sync.getCause());
            var async = assertInstanceOf(DunneleanTransportException.class,
                    failed(shortRequests.waitForCompletionAsync("request-timeout", Duration.ofSeconds(2), Duration.ofMillis(10))));
            assertInstanceOf(java.net.http.HttpTimeoutException.class, async.getCause());
            assertEquals("/v1/runs/request-timeout", takeRequest().path());
            assertEquals("/v1/runs/request-timeout", takeRequest().path());
            assertTrue(requests.isEmpty(), "request timeouts must not trigger replay or remote cancellation");
        } finally {
            release.countDown();
        }
    }

    @Test
    void interruptionStopsSynchronousWaitingWithoutCancellingTheRun() throws Exception {
        responder.set((exchange, request) -> reply(exchange, 200, runJson("RUNNING")));
        var outcome = new CompletableFuture<Throwable>();
        var waiting = new Thread(() -> {
            try {
                client.waitForCompletion("interrupted", Duration.ofSeconds(10), Duration.ofSeconds(3));
                outcome.complete(new AssertionError("waiting completed unexpectedly"));
            } catch (Throwable error) {
                outcome.complete(error);
            }
        }, "dunnelean-sdk-test-waiter");
        waiting.start();
        try {
            assertEquals("/v1/runs/interrupted", takeRequest().path());
            waiting.interrupt();
            assertInstanceOf(InterruptedException.class, await(outcome));
            waiting.join(1000);
            assertFalse(waiting.isAlive());
            assertTrue(requests.isEmpty());
        } finally {
            waiting.interrupt();
            waiting.join(1000);
        }
    }

    @Test
    void cancellingThePollingFutureStopsNewPollsWithoutRemoteCancellation() throws Exception {
        var release = new CountDownLatch(1);
        responder.set((exchange, request) -> {
            awaitRelease(release);
            reply(exchange, 200, runJson("RUNNING"));
        });
        var waiting = client.waitForCompletionAsync("local-cancel", Duration.ofSeconds(5), Duration.ofMillis(20));
        try {
            assertEquals("/v1/runs/local-cancel", takeRequest().path());
            assertTrue(waiting.cancel(true));
            release.countDown();
            assertNull(requests.poll(250, TimeUnit.MILLISECONDS), "cancellation must stop subsequent HTTP polls");
            assertTrue(waiting.isCancelled());
        } finally {
            release.countDown();
            waiting.cancel(true);
        }
    }

    private static void assertApiError(DunneleanApiException error) {
        assertEquals(409, error.statusCode());
        assertEquals("COMMIT_UNKNOWN", error.error().code());
        assertEquals("响应丢失", error.error().message());
        assertTrue(error.error().commitUnknown());
        assertFalse(error.error().retryable());
        assertEquals(ERROR, error.responseBody());
    }

    private static void assertRun(Run run) {
        assertEquals("run-fixture", run.runId());
        assertEquals(UINT64_MAX, run.rowsRead());
        assertEquals(UINT64_MAX, run.bytesRead());
        assertEquals(UINT64_MAX, run.rowsSubmitted());
        assertEquals(UINT64_MAX, run.rowsCommitted());
        assertEquals(UINT64_MAX, run.rowsFiltered());
        assertEquals(UINT64_MAX, run.serverAffectedRows());
        assertEquals(UINT64_MAX, run.batchesCommitted());
        assertEquals(Instant.parse("2026-09-30T01:00:00Z"), run.createdAt());
        assertEquals("[REDACTED]", run.config().path("password").asText());
    }

    private void assertRequest(String method, String path, String query, boolean hasSpec) throws Exception {
        var request = takeRequest();
        assertEquals(method, request.method());
        assertEquals(path, request.path());
        assertEquals(query, request.query());
        if (hasSpec) {
            assertTrue(request.header("Content-Type").startsWith("application/json"));
            assertEquals(JsonSupport.MAPPER.readTree(RunSpecJson.toJson(RunSpecJson.fromJson(SPEC))),
                    JsonSupport.MAPPER.readTree(request.body()));
        } else {
            assertEquals("", request.body());
        }
    }

    private RecordedRequest takeRequest() throws InterruptedException {
        var request = requests.poll(3, TimeUnit.SECONDS);
        assertNotNull(request, "HTTP request was not observed");
        return request;
    }

    private void standardResponse(HttpExchange exchange, RecordedRequest request) throws IOException {
        String path = request.path();
        if (path.equals("/healthz")) {
            reply(exchange, 200, """
                    {"status":"ok","service":"Dunnelean","version":"0.1.0",
                     "state_store_id":"store-fixture","future_field":"ignored"}
                    """);
        } else if (path.equals("/readyz")) {
            reply(exchange, 200, "{\"status\":\"ready\"}");
        } else if (path.equals("/v1/connectors")) {
            reply(exchange, 200, """
                    {"connectors":[{"type":"mysql"}],"request_schema":{"future_keyword":true},"limits":{}}
                    """);
        } else if (path.equals("/openapi.json")) {
            reply(exchange, 200, "{\"openapi\":\"3.1.0\"}");
        } else if (path.equals("/v1/validate")) {
            reply(exchange, 200, """
                    {"valid":true,"source_schema":[{"name":"id","type":"UInt64","nullable":false}],
                     "target_schema":[{"name":"id","type":"UInt64","nullable":false}],"semantics":"batch commits"}
                    """);
        } else if (path.endsWith("/batches")) {
            reply(exchange, 200, """
                    {"batches":[{"batch_id":18446744073709551615,"state":"UNKNOWN",
                     "rows":18446744073709551615,"bytes":18446744073709551615,
                     "label":null,"detail":"opaque diagnostic"}]}
                    """);
        } else if (path.startsWith("/v1/requests/")) {
            boolean cancel = path.endsWith("/cancel");
            reply(exchange, 200, "{\"request_id\":\"request-fixture\",\"cancel_requested\":" + cancel
                    + ",\"run\":" + (cancel ? "null" : runJson("SUCCEEDED")) + "}");
        } else if (path.equals("/v1/runs") && request.method().equals("GET")) {
            reply(exchange, 200, "{\"runs\":[" + runJson("SUCCEEDED") + "]}");
        } else {
            reply(exchange, request.method().equals("POST") && path.equals("/v1/runs") ? 202 : 200,
                    runJson("SUCCEEDED"));
        }
    }

    private static String runJson(String state) {
        boolean failed = state.equals("FAILED");
        return """
                {"run_id":"run-fixture","request_id":"request-fixture","state":"%s","stage":"transfer",
                 "created_at":"2026-09-30T09:00:00+08:00","updated_at":"2026-09-30T01:00:01Z",
                 "rows_read":18446744073709551615,"bytes_read":18446744073709551615,
                 "rows_submitted":18446744073709551615,"rows_committed":18446744073709551615,
                 "rows_filtered":18446744073709551615,"server_affected_rows":18446744073709551615,
                 "batches_committed":18446744073709551615,"partial_write":%s,"commit_unknown":%s,
                 "error":%s,"config":{"password":"[REDACTED]"},"future_field":1}
                """.formatted(state, failed, failed,
                failed ? "{\"code\":\"COMMIT_UNKNOWN\",\"message\":\"uncertain\",\"commit_unknown\":true,\"retryable\":false}" : "null");
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        } finally {
            exchange.close();
        }
    }

    private static void awaitRelease(CountDownLatch release) throws IOException {
        try {
            if (!release.await(3, TimeUnit.SECONDS)) {
                throw new IOException("test response was not released");
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IOException(error);
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(3, TimeUnit.SECONDS);
    }

    private static Throwable failed(CompletableFuture<?> future) {
        return assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS)).getCause();
    }

    private record RecordedRequest(String method, String path, String query,
            Map<String, List<String>> headers, String body) {
        String header(String name) {
            return headers.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(name))
                    .flatMap(entry -> entry.getValue().stream()).findFirst().orElse(null);
        }
    }

    @FunctionalInterface
    private interface Responder {
        void respond(HttpExchange exchange, RecordedRequest request) throws IOException;
    }
}
