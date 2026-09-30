package io.github.casperfrome.dunnelean;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

/** Opt-in control-plane integration; it starts only its own isolated Rust service and needs no databases. */
class RustControlIT {
    @TempDir
    Path directory;
    private Process child;
    private DunneleanClient client;
    private String base;

    @BeforeEach
    void startIsolatedRustService() throws Exception {
        Path binary = binaryPath();
        assertTrue(Files.isRegularFile(binary), "Build Rust first or set -Ddunnelean.binary: " + binary);
        Path state = directory.resolve("state.sqlite");
        Path config = directory.resolve("dunnelean.toml");
        Path log = directory.resolve("rust-service.log");
        Files.writeString(config, "listen=\"127.0.0.1:0\"\nstate_path="
                + JsonSupport.MAPPER.writeValueAsString(state.toAbsolutePath().toString().replace('\\', '/'))
                + "\nmax_running=1\nmax_queued=1\nshutdown_timeout_ms=1000\n", StandardCharsets.UTF_8);
        var launch = new ProcessBuilder(binary.toString(), "serve", "--config", config.toString())
                .directory(directory.toFile()).redirectErrorStream(true).redirectOutput(log.toFile());
        launch.environment().put("RUST_LOG", "dunnelean=info");
        child = launch.start();
        var address = Pattern.compile("127\\.0\\.0\\.1:(\\d+)");
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            String output = Files.readString(log, StandardCharsets.UTF_8);
            var match = address.matcher(output);
            if (match.find()) {
                base = "http://127.0.0.1:" + match.group(1);
                client = DunneleanClient.builder().baseUrl(base).requestTimeout(Duration.ofSeconds(3)).build();
                client.health();
                return;
            }
            assertTrue(child.isAlive(), "Rust service stopped during startup:\n" + output);
            Thread.sleep(25);
        }
        fail("Rust service did not report its ephemeral address:\n" + Files.readString(log, StandardCharsets.UTF_8));
    }

    @AfterEach
    void stopOnlyTheOwnedRustProcess() throws Exception {
        if (child != null) {
            child.destroy();
            if (!child.waitFor(3, TimeUnit.SECONDS)) {
                child.destroyForcibly();
                assertTrue(child.waitFor(3, TimeUnit.SECONDS), "Owned Rust service did not stop");
            }
        }
    }

    @Test
    void realRustControlContractPreservesCancellationAndStoreIdentity() throws Exception {
        var health = client.health();
        assertEquals("ok", health.status());
        assertEquals("Dunnelean", health.service());
        assertFalse(health.stateStoreId().isBlank());
        assertEquals("ready", client.ready().status());
        var connectors = client.connectors();
        assertEquals(2, connectors.connectors().size());
        assertTrue(connectors.requestSchema().has("$defs"));
        assertEquals("3.1.0", client.openApi().path("openapi").asText());
        assertTrue(client.listRuns().runs().isEmpty());

        String requestId = "java-it-取消 /?#";
        var missing = assertThrows(DunneleanApiException.class, () -> client.getRequest(requestId));
        assertEquals(404, missing.statusCode());
        assertEquals("NOT_FOUND", missing.error().code());
        var cancellation = client.cancelRequest(requestId);
        assertEquals(requestId, cancellation.requestId());
        assertTrue(cancellation.cancelRequested());
        assertNull(cancellation.run());
        assertTrue(client.getRequest(requestId).cancelRequested());
        assertNull(client.getRequest(requestId).run());

        var spec = RunSpecJson.fromJson("""
                {"request_id":%s,
                 "reader":{"type":"mysql","connection":{"database":"source",
                  "credentials":{"username":"test","password":""}},"source":{"table":"orders"}},
                 "writer":{"type":"doris","sql":{"database":"target",
                  "credentials":{"username":"test","password":""}},
                  "fe_http_urls":["http://127.0.0.1:8030"],"table":"orders"}}
                """.formatted(JsonSupport.MAPPER.writeValueAsString(requestId)));
        var late = assertThrows(DunneleanApiException.class, () -> client.submit(spec));
        assertEquals(409, late.statusCode());
        assertEquals("REQUEST_CANCELLED", late.error().code());
        assertTrue(client.listRuns().runs().isEmpty());

        var wrongIdentity = DunneleanClient.builder().baseUrl(base).stateStoreId("different-state-store").build();
        var changed = assertThrows(DunneleanApiException.class, wrongIdentity::health);
        assertEquals(409, changed.statusCode());
        assertEquals("STATE_STORE_CHANGED", changed.error().code());
        var guarded = DunneleanClient.builder().baseUrl(base).stateStoreId(health.stateStoreId()).build();
        assertTrue(guarded.getRequest(requestId).cancelRequested());
    }

    private static Path binaryPath() {
        String override = System.getProperty("dunnelean.binary");
        if (override != null && !override.isBlank()) {
            return Path.of(override).toAbsolutePath().normalize();
        }
        Path root = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (root != null && !(Files.isRegularFile(root.resolve("Cargo.toml"))
                && Files.isRegularFile(root.resolve("src/api.rs")))) {
            root = root.getParent();
        }
        assertNotNull(root, "Cannot locate repository; set -Ddunnelean.binary explicitly");
        String filename = System.getProperty("os.name").startsWith("Windows") ? "dunnelean.exe" : "dunnelean";
        return root.resolve("target/release").resolve(filename);
    }
}
