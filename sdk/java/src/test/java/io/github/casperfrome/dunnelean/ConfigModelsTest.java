package io.github.casperfrome.dunnelean;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigModelsTest {
    private static final BigInteger U64_MAX = new BigInteger("18446744073709551615");
    private static Credentials credentials() { return Credentials.builder().username("sdk-user").password("secret-not-for-logs").build(); }
    private static MysqlConnection connection() { return MysqlConnection.builder().database("test").credentials(credentials()).build(); }
    private static RunSpec spec() {
        return RunSpec.builder()
                .reader(MysqlReader.builder().connection(connection()).source(Source.builder().table("source").build()).build())
                .writer(DorisWriter.builder().sql(connection()).table("target").feHttpUrls(List.of("http://localhost:8030")).build())
                .build();
    }

    @Test void defaultsMatchRustConfig() {
        assertEquals(new Timeouts(BigInteger.valueOf(10_000), BigInteger.valueOf(60_000), BigInteger.valueOf(120_000)), Timeouts.builder().build());
        assertEquals(new Batch(BigInteger.valueOf(10_000), BigInteger.valueOf(16L * 1024 * 1024)), Batch.builder().build());
        MysqlConnection connection = connection();
        assertEquals("127.0.0.1", connection.host());
        assertEquals(3306, connection.port());
        assertEquals("utf8mb4", connection.charset());
        assertEquals("+00:00", connection.timeZone());
        assertEquals(BigInteger.valueOf(64L * 1024 * 1024), connection.maxAllowedPacketBytes());
        assertFalse(connection.tls().enabled());
        assertEquals(Map.of(), connection.sessionVariables());
        Execution execution = Execution.builder().build();
        assertEquals(BigInteger.valueOf(3_600_000), execution.timeoutMs());
        assertEquals(BigInteger.valueOf(4), execution.queueCapacity());
        assertEquals(BigInteger.valueOf(256L * 1024 * 1024), execution.memoryBytes());
        assertEquals(BigInteger.valueOf(24L * 1024 * 1024), execution.maxRowBytes());
        assertNull(execution.rowsPerSecond());
        assertNull(execution.bytesPerSecond());
        WriterOptions options = WriterOptions.builder().build();
        assertEquals(1, options.parallelism());
        assertEquals(BigInteger.valueOf(2), options.retryAttempts());
        assertEquals(BigInteger.valueOf(250), options.retryBackoffMs());
        assertEquals(List.of(), options.preSql());
        assertEquals(List.of(), options.postSql());
        assertEquals(Consistency.SNAPSHOT, ((MysqlReader) spec().reader()).consistency());
        DorisWriter doris = (DorisWriter) spec().writer();
        assertEquals(DorisWriteMode.APPEND, doris.mode());
        assertEquals("dunnelean", doris.labelPrefix());
        assertEquals(BigInteger.valueOf(120), doris.loadTimeoutSeconds());
        assertTrue(doris.strictMode());
        assertEquals(0.0, doris.maxFilterRatio());
        assertEquals("+00:00", doris.timeZone());
        assertEquals(BigInteger.valueOf(2L * 1024 * 1024 * 1024), doris.execMemLimit());
        DorisReader reader = DorisReader.builder().flightUri("grpc://localhost:8070").database("test").credentials(credentials()).source(Source.builder().table("t").build()).build();
        assertEquals(1, reader.endpointParallelism());
        assertEquals(BigInteger.valueOf(64L * 1024 * 1024), reader.maxGrpcMessageBytes());
        assertEquals(MysqlWriteMode.INSERT, MysqlWriter.builder().connection(connection).table("t").build().mode());
    }

    @Test void allParameterVariantsUseExactWireTypes() throws Exception {
        List<Parameter> parameters = List.of(Parameter.nullValue(), Parameter.string("中文\""), Parameter.i64(Long.MIN_VALUE), Parameter.u64(U64_MAX),
                Parameter.decimal(new BigDecimal("12345678901234567890.00100")), Parameter.f64(0.25), Parameter.bool(true), Parameter.binary(new byte[]{0, (byte) 255}));
        Source source = Source.builder().query("select ?, ?, ?, ?, ?, ?, ?, ?").params(parameters).build();
        RunSpec original = spec().toBuilder().reader(((MysqlReader) spec().reader()).toBuilder().source(source).build()).build();
        JsonNode tree = JsonSupport.MAPPER.readTree(RunSpecJson.toJson(original));
        JsonNode params = tree.path("reader").path("source").path("params");
        assertEquals("{\"type\":\"null\"}", params.get(0).toString());
        assertTrue(params.get(2).path("value").isTextual());
        assertEquals(Long.toString(Long.MIN_VALUE), params.get(2).path("value").textValue());
        assertEquals(U64_MAX.toString(), params.get(3).path("value").textValue());
        assertEquals("12345678901234567890.00100", params.get(4).path("value").textValue());
        assertTrue(params.get(5).path("value").isNumber());
        assertTrue(params.get(6).path("value").isBoolean());
        assertEquals("AP8=", params.get(7).path("value").textValue());
        assertEquals(original, RunSpecJson.fromJson(tree.toString()));
    }

    @Test void unsignedConfigAndStringBoundsPreserveMaximum() throws Exception {
        RunSpec original = spec().toBuilder().execution(Execution.builder().timeoutMs(U64_MAX).rowsPerSecond(U64_MAX).bytesPerSecond(U64_MAX).build())
                .reader(((MysqlReader) spec().reader()).toBuilder().split(Split.builder().column("id").lowerBound(Long.MIN_VALUE).upperBound(U64_MAX).partitions(4).build())
                        .batch(Batch.builder().rows(U64_MAX).bytes(U64_MAX).build()).build()).build();
        JsonNode tree = JsonSupport.MAPPER.readTree(RunSpecJson.toJson(original));
        assertTrue(tree.path("execution").path("timeout_ms").isIntegralNumber());
        assertEquals(U64_MAX, tree.path("execution").path("timeout_ms").bigIntegerValue());
        assertTrue(tree.path("reader").path("split").path("upper_bound").isTextual());
        assertEquals(original, RunSpecJson.fromTree(tree));
    }

    @Test void credentialsAreExplicitAndAlwaysRedactedInNestedToString() {
        assertThrows(IllegalArgumentException.class, () -> Credentials.builder().username("user").build());
        assertThrows(IllegalArgumentException.class, () -> Credentials.builder().username("user").password("p").passwordEnv("PASSWORD").build());
        assertEquals("", Credentials.builder().username("user").password("").build().password());
        assertNull(Credentials.builder().username("user").passwordEnv("NOT_RESOLVED_ON_CLIENT").build().password());
        assertFalse(spec().toString().contains("secret-not-for-logs"));
        assertTrue(spec().toString().contains("[REDACTED]"));
    }

    @Test void collectionsAreCopiedAndToBuilderPreservesOriginal() {
        List<String> columns = new ArrayList<>(List.of("id"));
        Map<String, String> types = new HashMap<>(Map.of("id", "UInt64"));
        Source original = Source.builder().table("source").columns(columns).columnTypes(types).build();
        columns.add("unexpected"); types.put("extra", "Utf8");
        assertEquals(List.of("id"), original.columns());
        assertEquals(Map.of("id", "UInt64"), original.columnTypes());
        assertThrows(UnsupportedOperationException.class, () -> original.columns().add("new"));
        assertThrows(UnsupportedOperationException.class, () -> original.columnTypes().put("new", "Utf8"));
        assertEquals("source", original.table());
        assertEquals("updated", original.toBuilder().table("updated").build().table());
        assertEquals(spec(), spec().toBuilder().build());
    }

    @Test void invalidRangesAndNonFiniteParametersFailLocally() {
        assertThrows(IllegalArgumentException.class, () -> Parameter.i64("9223372036854775808"));
        assertThrows(IllegalArgumentException.class, () -> Parameter.u64("18446744073709551616"));
        assertThrows(IllegalArgumentException.class, () -> Parameter.u64(-1));
        assertThrows(IllegalArgumentException.class, () -> Parameter.i64(" 1"));
        assertThrows(IllegalArgumentException.class, () -> Parameter.decimal("not-a-number"));
        assertThrows(IllegalArgumentException.class, () -> Parameter.binary("!not-base64!"));
        assertThrows(IllegalArgumentException.class, () -> Parameter.f64(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> Parameter.f64(Double.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> Batch.builder().rows(U64_MAX.add(BigInteger.ONE)).build());
        assertThrows(IllegalArgumentException.class, () -> Timeouts.builder().connectMs(-1).build());
        assertThrows(IllegalArgumentException.class, () -> connection().toBuilder().port(65536).build());
        assertThrows(IllegalArgumentException.class, () -> Split.builder().column("id").lowerBound("-9223372036854775809").upperBound("10").partitions(1).build());
        assertThrows(IllegalArgumentException.class, () -> Split.builder().column("id").lowerBound("0").upperBound("10").partitions(1025).build());
    }

    @Test void requestIdUsesUtf8BytesRatherThanJavaCharacterCount() {
        assertEquals("中".repeat(66), spec().toBuilder().requestId("中".repeat(66)).build().requestId());
        assertThrows(IllegalArgumentException.class, () -> spec().toBuilder().requestId("中".repeat(67)).build());
        assertThrows(IllegalArgumentException.class, () -> spec().toBuilder().requestId("").build());
    }
}
