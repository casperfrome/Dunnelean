package io.github.casperfrome.dunnelean;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResponseModelsTest {
    private static final BigInteger U64_MAX = new BigInteger("18446744073709551615");

    private static final String RUN_JSON = """
            {
              "run_id":"run-1", "request_id":null, "state":"SUCCEEDED", "stage":"complete",
              "created_at":"2026-09-30T02:03:04.123456Z", "updated_at":"2026-09-30T10:03:05+08:00",
              "rows_read":18446744073709551615, "bytes_read":18446744073709551615,
              "rows_submitted":18446744073709551615, "rows_committed":18446744073709551615,
              "rows_filtered":18446744073709551615, "server_affected_rows":18446744073709551615,
              "batches_committed":18446744073709551615, "partial_write":false, "commit_unknown":false,
              "error":null, "config":{"reader":{"connection":{"password":"***"}}},
              "future_counter":123
            }
            """;

    @Test
    void healthReadyAndValidationMatchActualServiceShapes() throws Exception {
        Health health = read("""
                {"status":"ok","service":"Dunnelean","version":"0.1.0","state_store_id":"store-1","future":true}
                """, Health.class);
        assertEquals("ok", health.status());
        assertEquals("Dunnelean", health.service());
        assertEquals("0.1.0", health.version());
        assertEquals("store-1", health.stateStoreId());
        assertEquals("ready", read("{\"status\":\"ready\",\"future\":true}", Ready.class).status());

        Validation validation = read("""
                {"valid":true,
                 "source_schema":[{"name":"id","type":"UInt64","nullable":false,"future":0}],
                 "target_schema":[{"name":"id","type":"UInt64","nullable":false}],
                 "semantics":"batch commits; no whole-job rollback or automatic resume","future":0}
                """, Validation.class);
        assertTrue(validation.valid());
        assertEquals(new Field("id", "UInt64", false), validation.sourceSchema().get(0));
        assertEquals(validation.sourceSchema(), validation.targetSchema());
        assertEquals("batch commits; no whole-job rollback or automatic resume", validation.semantics());
        assertThrows(UnsupportedOperationException.class,
                () -> validation.sourceSchema().add(new Field("x", "Utf8", true)));
    }

    @Test
    void exactUnsignedRunCountersAndRfc3339TimestampsAreRetained() throws Exception {
        Run run = read(RUN_JSON, Run.class);
        assertEquals("run-1", run.runId());
        assertNull(run.requestId());
        assertEquals("complete", run.stage());
        assertEquals(Instant.parse("2026-09-30T02:03:04.123456Z"), run.createdAt());
        assertEquals(Instant.parse("2026-09-30T02:03:05Z"), run.updatedAt());
        for (BigInteger counter : List.of(run.rowsRead(), run.bytesRead(), run.rowsSubmitted(), run.rowsCommitted(),
                run.rowsFiltered(), run.serverAffectedRows(), run.batchesCommitted())) {
            assertEquals(U64_MAX, counter);
        }
        assertFalse(run.partialWrite());
        assertFalse(run.commitUnknown());
        assertNull(run.error());
        assertEquals(RunState.SUCCEEDED, run.knownState().orElseThrow());
        assertTrue(run.terminal());
        JsonNode json = JsonSupport.MAPPER.valueToTree(run);
        assertEquals(U64_MAX, json.get("rows_read").bigIntegerValue());
        assertFalse(json.has("known_state"));
        assertFalse(json.has("terminal"));
    }

    @Test
    void knownAndFutureStatesStayDistinct() throws Exception {
        for (RunState state : RunState.values()) {
            Run run = read(RUN_JSON.replace("SUCCEEDED", state.name()), Run.class);
            assertEquals(state, run.knownState().orElseThrow());
            assertEquals(List.of("SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED").contains(state.name()),
                    run.terminal());
        }
        Run future = read(RUN_JSON.replace("SUCCEEDED", "PAUSED_BY_FUTURE_SERVICE"), Run.class);
        assertEquals("PAUSED_BY_FUTURE_SERVICE", future.state());
        assertTrue(future.knownState().isEmpty());
        assertFalse(future.terminal());
        assertTrue(RunState.from(null).isEmpty());
        assertTrue(RunState.from("succeeded").isEmpty());
    }

    @Test
    void requestCancellationTombstoneHasNullableRun() throws Exception {
        RequestStatus tombstone = read("""
                {"request_id":"request-1","cancel_requested":true,"run":null,"future":true}
                """, RequestStatus.class);
        assertEquals("request-1", tombstone.requestId());
        assertTrue(tombstone.cancelRequested());
        assertNull(tombstone.run());
        RequestStatus submitted = read("{\"request_id\":\"request-1\",\"cancel_requested\":false,\"run\":"
                + RUN_JSON + "}", RequestStatus.class);
        assertEquals("run-1", submitted.run().runId());
        assertFalse(submitted.cancelRequested());
    }

    @Test
    void batchDetailsRemainNullableSerializedStrings() throws Exception {
        BatchList list = read("""
                {"batches":[
                  {"batch_id":18446744073709551615,"state":"CONFIRMED","rows":18446744073709551615,
                   "bytes":18446744073709551615,"label":"load-1","detail":"{\\"loaded\\":123}","future":1},
                  {"batch_id":2,"state":"NEW_FUTURE_STATE","rows":0,"bytes":0,"label":null,"detail":null}
                ],"future":true}
                """, BatchList.class);
        BatchReceipt first = list.batches().get(0);
        assertEquals(U64_MAX, first.batchId());
        assertEquals(U64_MAX, first.rows());
        assertEquals(U64_MAX, first.bytes());
        assertEquals("load-1", first.label());
        assertEquals("{\"loaded\":123}", first.detail());
        assertEquals(BatchState.CONFIRMED, first.knownState().orElseThrow());
        BatchReceipt second = list.batches().get(1);
        assertEquals("NEW_FUTURE_STATE", second.state());
        assertTrue(second.knownState().isEmpty());
        assertNull(second.label());
        assertNull(second.detail());
        assertTrue(BatchState.from(null).isEmpty());
        for (BatchState state : BatchState.values()) {
            assertEquals(state, BatchState.from(state.name()).orElseThrow());
        }
        assertThrows(UnsupportedOperationException.class, () -> list.batches().clear());
    }

    @Test
    void runListAndSchemaListsCopyMutableInputs() throws Exception {
        Run run = read(RUN_JSON, Run.class);
        ArrayList<Run> runs = new ArrayList<>(List.of(run));
        RunList runList = new RunList(runs);
        runs.clear();
        assertEquals(List.of(run), runList.runs());
        assertThrows(UnsupportedOperationException.class, () -> runList.runs().clear());
        RunList decoded = read("{\"runs\":[" + RUN_JSON + "],\"future\":true}", RunList.class);
        assertEquals(run, decoded.runs().get(0));

        ArrayList<Field> fields = new ArrayList<>(List.of(new Field("id", "Int64", true)));
        Validation validation = new Validation(true, fields, fields, "batch commits");
        fields.clear();
        assertEquals(1, validation.sourceSchema().size());
        assertEquals(1, validation.targetSchema().size());

        ArrayList<BatchReceipt> batches = new ArrayList<>(List.of(new BatchReceipt(BigInteger.ONE, "INTENT",
                BigInteger.TEN, BigInteger.ZERO, null, null)));
        BatchList batchList = new BatchList(batches);
        batches.clear();
        assertEquals(1, batchList.batches().size());
    }

    @Test
    void runConfigurationIsDefensivelyCopied() throws Exception {
        Run decoded = read(RUN_JSON, Run.class);
        JsonNode supplied = decoded.config();
        Run run = new Run(decoded.runId(), decoded.requestId(), decoded.state(), decoded.stage(),
                decoded.createdAt(), decoded.updatedAt(), decoded.rowsRead(), decoded.bytesRead(),
                decoded.rowsSubmitted(), decoded.rowsCommitted(), decoded.rowsFiltered(), decoded.serverAffectedRows(),
                decoded.batchesCommitted(), decoded.partialWrite(), decoded.commitUnknown(), decoded.error(), supplied);
        ((ObjectNode) supplied).removeAll();
        assertEquals("***", run.config().at("/reader/connection/password").textValue());
        ((ObjectNode) run.config()).removeAll();
        assertEquals("***", run.config().at("/reader/connection/password").textValue());
    }

    @Test
    void dynamicConnectorSchemaIsCopiedOnInputAndOutput() throws Exception {
        Connectors decoded = read("""
                {"connectors":[{"type":"mysql","reader":{"protocol":"binary prepared streaming"}}],
                 "request_schema":{"type":"object"},"limits":{"resume":false},"future":true}
                """, Connectors.class);
        assertEquals("mysql", decoded.connectors().get(0).get("type").textValue());
        ArrayList<JsonNode> connectors = new ArrayList<>(decoded.connectors());
        JsonNode schema = decoded.requestSchema();
        JsonNode limits = decoded.limits();
        Connectors copy = new Connectors(connectors, schema, limits);
        ((ObjectNode) connectors.get(0)).removeAll();
        connectors.clear();
        ((ObjectNode) schema).removeAll();
        ((ObjectNode) limits).removeAll();
        assertEquals("mysql", copy.connectors().get(0).get("type").textValue());
        assertEquals("object", copy.requestSchema().get("type").textValue());
        assertFalse(copy.limits().get("resume").booleanValue());
        ((ObjectNode) copy.connectors().get(0)).removeAll();
        ((ObjectNode) copy.requestSchema()).removeAll();
        ((ObjectNode) copy.limits()).removeAll();
        assertEquals("mysql", copy.connectors().get(0).get("type").textValue());
        assertTrue(copy.requestSchema().has("type"));
        assertTrue(copy.limits().has("resume"));
        assertThrows(UnsupportedOperationException.class, () -> copy.connectors().clear());
    }

    @Test
    void embeddedRunErrorRetainsCommitFlagsAndFutureFields() throws Exception {
        String error = """
                {"code":"COMMIT_UNKNOWN","message":"Outcome unknown","commit_unknown":true,"retryable":false,"future":1}
                """;
        ApiError decoded = read(error, ApiError.class);
        assertEquals("COMMIT_UNKNOWN", decoded.code());
        assertEquals("Outcome unknown", decoded.message());
        assertTrue(decoded.commitUnknown());
        assertFalse(decoded.retryable());
        Run failed = read(RUN_JSON.replace("SUCCEEDED", "FAILED").replace("\"error\":null", "\"error\":" + error), Run.class);
        assertEquals(decoded, failed.error());
        assertTrue(failed.terminal());
    }

    @Test
    void successfulEmptyObjectsAndMissingRequiredBooleansAreRejected() {
        for (Class<?> type : List.of(Health.class, Ready.class, Validation.class, Field.class, Run.class,
                RunList.class, BatchReceipt.class, BatchList.class, RequestStatus.class, Connectors.class, ApiError.class)) {
            assertThrows(JsonProcessingException.class, () -> read("{}", type), type.getSimpleName());
        }
        assertThrows(JsonProcessingException.class,
                () -> read("{\"request_id\":\"request-1\",\"run\":null}", RequestStatus.class));
        assertThrows(JsonProcessingException.class,
                () -> read("{\"name\":\"id\",\"type\":\"Int64\"}", Field.class));
        assertThrows(JsonProcessingException.class,
                () -> read(RUN_JSON.replace("\"partial_write\":false,", ""), Run.class));
        assertThrows(JsonProcessingException.class,
                () -> read("{\"code\":\"IO\",\"message\":\"failure\",\"retryable\":false}", ApiError.class));
    }

    @Test
    void malformedCounterAndBooleanValuesAreRejected() {
        assertThrows(JsonProcessingException.class,
                () -> read(RUN_JSON.replace("\"rows_read\":18446744073709551615", "\"rows_read\":-1"), Run.class));
        assertThrows(JsonProcessingException.class,
                () -> read(RUN_JSON.replace("\"rows_read\":18446744073709551615", "\"rows_read\":18446744073709551616"), Run.class));
        assertThrows(JsonProcessingException.class,
                () -> read(RUN_JSON.replace("\"rows_read\":18446744073709551615", "\"rows_read\":1.5"), Run.class));
        assertThrows(JsonProcessingException.class,
                () -> read(RUN_JSON.replace("\"partial_write\":false", "\"partial_write\":null"), Run.class));
        assertThrows(JsonProcessingException.class,
                () -> read("{\"batch_id\":-1,\"state\":\"INTENT\",\"rows\":0,\"bytes\":0}", BatchReceipt.class));
    }

    @Test
    void sdkExceptionsPreserveTypedFailureInformation() {
        ApiError error = new ApiError("COMMIT_UNKNOWN", "Outcome unknown", true, true);
        DunneleanApiException api = new DunneleanApiException(500, error, "bounded response");
        assertEquals(500, api.statusCode());
        assertSame(error, api.error());
        assertEquals("bounded response", api.responseBody());
        assertEquals(error.code(), api.code());
        assertEquals(error.message(), api.message());
        assertTrue(api.commitUnknown());
        assertTrue(api.retryable());
        DunneleanApiException plain = new DunneleanApiException(502, null, "bad gateway");
        assertNull(plain.error());
        assertNull(plain.code());
        assertEquals(plain.getMessage(), plain.message());
        assertFalse(plain.commitUnknown());
        assertFalse(plain.retryable());

        IOException cause = new IOException("connection reset");
        assertSame(cause, new DunneleanTransportException(cause).getCause());
        DunneleanProtocolException protocol = new DunneleanProtocolException(200, "not json", cause);
        assertSame(cause, protocol.getCause());
        assertEquals(200, protocol.statusCode());
        assertEquals("not json", protocol.responseBody());
        DunneleanTimeoutException timeout = new DunneleanTimeoutException("run-1", Duration.ofSeconds(2));
        assertEquals("run-1", timeout.runId());
        assertEquals(Duration.ofSeconds(2), timeout.timeout());
        assertInstanceOf(DunneleanException.class, timeout);
    }

    private static <T> T read(String json, Class<T> type) throws JsonProcessingException {
        return JsonSupport.MAPPER.readValue(json, type);
    }
}
