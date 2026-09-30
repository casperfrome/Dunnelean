import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.regex.*;

/** Minimal Java 17+ client. JSON handling is intentionally dependency-free. */
public final class DunneleanClient {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final String base;
    public DunneleanClient(String base) { this.base = base; }
    public String request(String method, String path, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMinutes(3));
        builder.header("Content-Type", "application/json");
        builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) throw new IllegalStateException(response.statusCode() + ": " + response.body());
        return response.body();
    }
    private static String field(String json, String name) {
        var m = Pattern.compile("\"" + name + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        if (!m.find()) throw new IllegalStateException("Missing field: " + name);
        return m.group(1);
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) throw new IllegalArgumentException("Usage: java DunneleanClient job.json [--cancel|--deduplicate]");
        var client = new DunneleanClient(System.getenv().getOrDefault("DUNNELEAN_URL", "http://127.0.0.1:9876"));
        String spec = Files.readString(Path.of(args[0]));
        System.out.println(client.request("POST", "/v1/validate", spec));
        String first = client.request("POST", "/v1/runs", spec);
        String id = field(first, "run_id");
        System.out.println("run_id=" + id);
        if (args.length > 1 && args[1].equals("--deduplicate")) {
            if (!Pattern.compile("\"request_id\"\\s*:\\s*\"").matcher(spec).find())
                throw new IllegalArgumentException("--deduplicate requires a request_id in job.json");
            String repeated = field(client.request("POST", "/v1/runs", spec), "run_id");
            if (!id.equals(repeated)) throw new IllegalStateException("Idempotency contract violated");
            System.out.println("Duplicate request returned the same run_id");
        }
        if (args.length > 1 && args[1].equals("--cancel")) {
            client.request("POST", "/v1/runs/" + id + "/cancel", null);
        }
        for (int i = 0; i < 7200; i++) {
            String result = client.request("GET", "/v1/runs/" + id, null);
            String state = field(result, "state");
            if (state.matches("SUCCEEDED|FAILED|CANCELLED|INTERRUPTED")) {
                System.out.println(result);
                if (!state.equals("SUCCEEDED") && !(args.length > 1 && state.equals("CANCELLED"))) System.exit(1);
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Polling deadline exceeded; query the run before deciding to resubmit");
    }
}
