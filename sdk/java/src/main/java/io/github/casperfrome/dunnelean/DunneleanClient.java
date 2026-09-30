package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thread-safe HTTP control client. Reuse one instance across tasks. The SDK does
 * not retry submissions or refresh a configured state-store identity.
 */
public final class DunneleanClient {
    private static final int MAX_DIAGNOSTIC_CHARS = 65_536;
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();
    private final URI baseUrl;
    private final HttpClient http;
    private final Duration requestTimeout;
    private final String stateStoreId;

    private DunneleanClient(Builder builder) {
        baseUrl = normalizeBase(builder.baseUrl);
        requestTimeout = positive(builder.requestTimeout, "requestTimeout");
        positive(builder.connectTimeout, "connectTimeout");
        http = builder.httpClient != null ? builder.httpClient : HttpClient.newBuilder()
                .connectTimeout(builder.connectTimeout).followRedirects(HttpClient.Redirect.NEVER).build();
        stateStoreId = builder.stateStoreId;
        if (stateStoreId != null && stateStoreId.isBlank()) {
            throw new IllegalArgumentException("stateStoreId must not be blank");
        }
    }

    /** Creates a builder using localhost:9876, 10s connect and 3min request timeouts. */
    public static Builder builder() { return new Builder(); }

    public Health health() throws InterruptedException { return get("/healthz", Health.class); }
    public CompletableFuture<Health> healthAsync() { return getAsync("/healthz", Health.class); }
    public Ready ready() throws InterruptedException { return get("/readyz", Ready.class); }
    public CompletableFuture<Ready> readyAsync() { return getAsync("/readyz", Ready.class); }
    public Connectors connectors() throws InterruptedException { return get("/v1/connectors", Connectors.class); }
    public CompletableFuture<Connectors> connectorsAsync() { return getAsync("/v1/connectors", Connectors.class); }
    public JsonNode openApi() throws InterruptedException { return get("/openapi.json", JsonNode.class); }
    public CompletableFuture<JsonNode> openApiAsync() { return getAsync("/openapi.json", JsonNode.class); }
    public Validation validate(RunSpec spec) throws InterruptedException {
        return send("POST", "/v1/validate", Objects.requireNonNull(spec, "spec"), Validation.class);
    }
    public CompletableFuture<Validation> validateAsync(RunSpec spec) {
        return sendAsync("POST", "/v1/validate", Objects.requireNonNull(spec, "spec"), Validation.class);
    }
    /** Submits once. Persist the request ID and query it when the response is lost. */
    public Run submit(RunSpec spec) throws InterruptedException {
        return send("POST", "/v1/runs", Objects.requireNonNull(spec, "spec"), Run.class);
    }
    public CompletableFuture<Run> submitAsync(RunSpec spec) {
        return sendAsync("POST", "/v1/runs", Objects.requireNonNull(spec, "spec"), Run.class);
    }
    public RunList listRuns() throws InterruptedException { return listRuns(50, 0); }
    public CompletableFuture<RunList> listRunsAsync() { return listRunsAsync(50, 0); }
    public RunList listRuns(int limit, long offset) throws InterruptedException {
        return get(pagination(limit, offset), RunList.class);
    }
    public CompletableFuture<RunList> listRunsAsync(int limit, long offset) {
        return getAsync(pagination(limit, offset), RunList.class);
    }
    public Run getRun(String runId) throws InterruptedException { return get(runPath(runId), Run.class); }
    public CompletableFuture<Run> getRunAsync(String runId) { return getAsync(runPath(runId), Run.class); }
    /** Requests stopping; the returned run may still be CANCELLING. */
    public Run cancelRun(String runId) throws InterruptedException {
        return send("POST", runPath(runId) + "/cancel", null, Run.class);
    }
    public CompletableFuture<Run> cancelRunAsync(String runId) {
        return sendAsync("POST", runPath(runId) + "/cancel", null, Run.class);
    }
    public BatchList listBatches(String runId) throws InterruptedException {
        return get(runPath(runId) + "/batches", BatchList.class);
    }
    public CompletableFuture<BatchList> listBatchesAsync(String runId) {
        return getAsync(runPath(runId) + "/batches", BatchList.class);
    }
    public RequestStatus getRequest(String requestId) throws InterruptedException {
        return get(requestPath(requestId), RequestStatus.class);
    }
    public CompletableFuture<RequestStatus> getRequestAsync(String requestId) {
        return getAsync(requestPath(requestId), RequestStatus.class);
    }
    /** Persists a cancellation even when the request has not yet been submitted. */
    public RequestStatus cancelRequest(String requestId) throws InterruptedException {
        return send("POST", requestPath(requestId) + "/cancel", null, RequestStatus.class);
    }
    public CompletableFuture<RequestStatus> cancelRequestAsync(String requestId) {
        return sendAsync("POST", requestPath(requestId) + "/cancel", null, RequestStatus.class);
    }

    /**
     * Waits for any terminal state, including FAILED. Timeout/interruption stops
     * waiting and does not cancel or resubmit the remote run.
     */
    public Run waitForCompletion(String runId, Duration timeout, Duration interval) throws InterruptedException {
        CompletableFuture<Run> waiting = waitForCompletionAsync(runId, timeout, interval);
        try {
            return waiting.get();
        } catch (InterruptedException e) {
            waiting.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw runtimeCause(e.getCause());
        }
    }

    /** Nonblocking polling. Cancelling the future stops local polling only. */
    public CompletableFuture<Run> waitForCompletionAsync(String runId, Duration timeout, Duration interval) {
        String path = runPath(runId);
        long budget = nanos(positive(timeout, "timeout"), "timeout");
        long delay = nanos(positive(interval, "interval"), "interval");
        return new Polling(path, runId, timeout, budget, delay).start();
    }

    private <T> T get(String path, Class<T> type) throws InterruptedException { return send("GET", path, null, type); }
    private <T> CompletableFuture<T> getAsync(String path, Class<T> type) { return sendAsync("GET", path, null, type); }

    private <T> T send(String method, String path, RunSpec body, Class<T> type) throws InterruptedException {
        HttpRequest request = request(method, path, body, requestTimeout);
        try {
            return decode(http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)), type);
        } catch (IOException e) {
            throw new DunneleanTransportException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private <T> CompletableFuture<T> sendAsync(String method, String path, RunSpec body, Class<T> type) {
        return sendAsync(method, path, body, type, requestTimeout);
    }

    private <T> CompletableFuture<T> sendAsync(String method, String path, RunSpec body, Class<T> type, Duration timeout) {
        try {
            var transport = http.sendAsync(request(method, path, body, timeout),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            var result = new CompletableFuture<T>();
            result.whenComplete((ignored, failure) -> {
                if (result.isCancelled()) transport.cancel(true);
            });
            transport.whenComplete((response, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(new DunneleanTransportException(unwrap(failure)));
                } else {
                    try { result.complete(decode(response, type)); }
                    catch (RuntimeException e) { result.completeExceptionally(e); }
                }
            });
            return result;
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private HttpRequest request(String method, String path, RunSpec body, Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(baseUrl.toASCIIString() + path))
                .timeout(timeout).header("Accept", "application/json");
        if (stateStoreId != null) builder.header("x-dunnelean-state-store-id", stateStoreId);
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.noBody();
        if (body != null) {
            builder.header("Content-Type", "application/json; charset=UTF-8");
            try {
                publisher = HttpRequest.BodyPublishers.ofString(RunSpecJson.toJson(body), StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("RunSpec could not be serialized", e);
            }
        }
        return builder.method(method, publisher).build();
    }

    private <T> T decode(HttpResponse<String> response, Class<T> type) {
        int status = response.statusCode();
        String body = response.body();
        if (status < 200 || status >= 300) {
            ApiError error = null;
            try {
                JsonNode envelope = JsonSupport.MAPPER.readTree(body);
                if (envelope != null && envelope.hasNonNull("error")) {
                    error = JsonSupport.MAPPER.treeToValue(envelope.get("error"), ApiError.class);
                }
            } catch (JsonProcessingException | IllegalArgumentException ignored) {
                // Reverse proxies and HTTP rejections may not use the service's error envelope.
            }
            throw new DunneleanApiException(status, error, diagnostic(body));
        }
        try {
            T value = JsonSupport.MAPPER.readValue(body, type);
            if (value == null || value instanceof JsonNode node && !node.isObject()) {
                throw new IllegalArgumentException("Expected a JSON response object");
            }
            return value;
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new DunneleanProtocolException(status, diagnostic(body), e);
        }
    }

    private static String diagnostic(String body) {
        if (body == null) return "";
        return body.length() <= MAX_DIAGNOSTIC_CHARS ? body : body.substring(0, MAX_DIAGNOSTIC_CHARS) + " [truncated]";
    }

    private static String runPath(String id) { return "/v1/runs/" + segment(id); }
    private static String requestPath(String id) {
        Objects.requireNonNull(id, "requestId");
        int bytes = id.getBytes(StandardCharsets.UTF_8).length;
        if (bytes == 0 || bytes > 200) throw new IllegalArgumentException("requestId must be 1..200 UTF-8 bytes");
        return "/v1/requests/" + segment(id);
    }
    private static String segment(String id) {
        if (Objects.requireNonNull(id, "id").isEmpty()) throw new IllegalArgumentException("id must not be empty");
        var encoded = new StringBuilder();
        boolean dots = id.equals(".") || id.equals("..");
        for (byte b : id.getBytes(StandardCharsets.UTF_8)) {
            int value = b & 255;
            if (value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
                    || value == '-' || value == '_' || value == '~' || value == '.' && !dots) {
                encoded.append((char) value);
            } else {
                encoded.append('%').append(HEX[value >>> 4]).append(HEX[value & 15]);
            }
        }
        return encoded.toString();
    }
    private static String pagination(int limit, long offset) {
        if (limit < 1 || limit > 500 || offset < 0) throw new IllegalArgumentException("limit must be 1..500 and offset nonnegative");
        return "/v1/runs?limit=" + limit + "&offset=" + offset;
    }
    private static URI normalizeBase(URI base) {
        Objects.requireNonNull(base, "baseUrl");
        if (!("http".equalsIgnoreCase(base.getScheme()) || "https".equalsIgnoreCase(base.getScheme()))
                || base.getHost() == null || base.getRawUserInfo() != null || base.getRawQuery() != null
                || base.getRawFragment() != null) {
            throw new IllegalArgumentException("baseUrl must be an HTTP(S) URL without credentials, query or fragment");
        }
        String value = base.toASCIIString();
        while (value.endsWith("/")) value = value.substring(0, value.length() - 1);
        return URI.create(value);
    }
    private static Duration positive(Duration value, String name) {
        if (Objects.requireNonNull(value, name).isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
    private static long nanos(Duration value, String name) {
        try { return value.toNanos(); }
        catch (ArithmeticException e) { throw new IllegalArgumentException(name + " is too large", e); }
    }
    private static Throwable unwrap(Throwable cause) {
        while ((cause instanceof java.util.concurrent.CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) cause = cause.getCause();
        return cause;
    }
    private static RuntimeException runtimeCause(Throwable cause) {
        cause = unwrap(cause);
        if (cause instanceof RuntimeException runtime) return runtime;
        return new DunneleanTransportException(cause);
    }

    private final class Polling {
        private final String path;
        private final String runId;
        private final Duration timeout;
        private final long budget;
        private final long interval;
        private final long started = System.nanoTime();
        private final CompletableFuture<Run> result = new CompletableFuture<>();
        private CompletableFuture<Run> inFlight;

        private Polling(String path, String runId, Duration timeout, long budget, long interval) {
            this.path = path;
            this.runId = runId;
            this.timeout = timeout;
            this.budget = budget;
            this.interval = interval;
        }
        private CompletableFuture<Run> start() {
            // The JDK's delayed executor uses a daemon timer; no SDK executor needs closing.
            result.orTimeout(budget, TimeUnit.NANOSECONDS);
            result.whenComplete((value, failure) -> {
                synchronized (this) {
                    if (inFlight != null) inFlight.cancel(true);
                }
            });
            // Translate the independent deadline into an SDK timeout exception.
            var outward = new CompletableFuture<Run>();
            result.whenCompleteAsync((value, failure) -> {
                if (failure == null) outward.complete(value);
                else if (unwrap(failure) instanceof TimeoutException) outward.completeExceptionally(new DunneleanTimeoutException(runId, timeout));
                else outward.completeExceptionally(unwrap(failure));
            });
            outward.whenComplete((value, failure) -> {
                if (outward.isCancelled()) result.cancel(true);
            });
            poll();
            return outward;
        }
        private synchronized void poll() {
            if (result.isDone()) return;
            long remaining = budget - (System.nanoTime() - started);
            if (remaining <= 0) {
                result.completeExceptionally(new DunneleanTimeoutException(runId, timeout));
                return;
            }
            Duration attemptTimeout = Duration.ofNanos(remaining);
            boolean deadlineLimited = attemptTimeout.compareTo(requestTimeout) <= 0;
            if (!deadlineLimited) attemptTimeout = requestTimeout;
            inFlight = sendAsync("GET", path, null, Run.class, attemptTimeout);
            inFlight.whenComplete((run, failure) -> {
                if (result.isDone()) return;
                if (failure != null) {
                    Throwable cause = unwrap(failure);
                    Throwable transportCause = cause instanceof DunneleanTransportException ? cause.getCause() : cause;
                    boolean requestDeadlineExpired = deadlineLimited
                            && transportCause instanceof java.net.http.HttpTimeoutException
                            && (!(transportCause instanceof java.net.http.HttpConnectTimeoutException)
                                || http.connectTimeout().map(value -> value.compareTo(Duration.ofNanos(remaining)) >= 0).orElse(true));
                    // HttpClient's timer can fire just before our nanosecond deadline.
                    if (System.nanoTime() - started >= budget || requestDeadlineExpired) result.completeExceptionally(new DunneleanTimeoutException(runId, timeout));
                    else result.completeExceptionally(unwrap(failure));
                } else if (run.terminal()) {
                    result.complete(run);
                } else {
                    long next = Math.min(interval, Math.max(1, budget - (System.nanoTime() - started)));
                    CompletableFuture.delayedExecutor(next, TimeUnit.NANOSECONDS).execute(this::poll);
                }
            });
        }
    }

    /** Client options. An injected HttpClient owns its connect timeout and TLS settings. */
    public static final class Builder {
        private URI baseUrl = URI.create("http://127.0.0.1:9876");
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofMinutes(3);
        private HttpClient httpClient;
        private String stateStoreId;
        private Builder() { }
        public Builder baseUrl(String value) { return baseUrl(URI.create(value)); }
        public Builder baseUrl(URI value) { baseUrl = Objects.requireNonNull(value, "baseUrl"); return this; }
        public Builder connectTimeout(Duration value) { connectTimeout = value; return this; }
        public Builder requestTimeout(Duration value) { requestTimeout = value; return this; }
        public Builder httpClient(HttpClient value) { httpClient = Objects.requireNonNull(value, "httpClient"); return this; }
        public Builder stateStoreId(String value) { stateStoreId = value; return this; }
        public DunneleanClient build() { return new DunneleanClient(this); }
    }
}
