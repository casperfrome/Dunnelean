package io.github.casperfrome.dunnelean;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunSpecJsonTest {
    @TempDir Path temp;

    private static Path repository() {
        for (Path path = Path.of("").toAbsolutePath(); path != null; path = path.getParent()) if (Files.isRegularFile(path.resolve("examples/mysql-to-doris.json"))) return path;
        throw new AssertionError("Cannot locate repository examples");
    }
    private static RunSpec example(String file) throws Exception { return RunSpecJson.read(repository().resolve("examples").resolve(file)); }

    @Test void bothExistingExamplesRoundTripAndPreserveConnectorDefaults() throws Exception {
        RunSpec mysqlToDoris = example("mysql-to-doris.json");
        RunSpec dorisToMysql = example("doris-to-mysql.json");
        for (RunSpec spec : List.of(mysqlToDoris, dorisToMysql)) {
            assertEquals(spec, RunSpecJson.fromJson(RunSpecJson.toJson(spec)));
            assertEquals(Execution.builder().build(), spec.execution());
        }
        assertInstanceOf(MysqlReader.class, mysqlToDoris.reader());
        assertInstanceOf(DorisWriter.class, mysqlToDoris.writer());
        assertInstanceOf(DorisReader.class, dorisToMysql.reader());
        assertInstanceOf(MysqlWriter.class, dorisToMysql.writer());
        assertEquals(2, ((DorisReader) dorisToMysql.reader()).endpointMap().size());
        assertEquals("DUNNELEAN_TEST_PASSWORD", ((MysqlReader) mysqlToDoris.reader()).connection().credentials().passwordEnv());
        assertFalse(RunSpecJson.toJson(mysqlToDoris).contains("password\""));
    }

    @Test void writesAndReadsUtf8FilesAndWhereAlias() throws Exception {
        RunSpec spec = example("mysql-to-doris.json");
        MysqlReader reader = (MysqlReader) spec.reader();
        Source source = reader.source().toBuilder().predicate("name = '中文'").build();
        spec = spec.toBuilder().reader(reader.toBuilder().source(source).build()).requestId("请求-1").build();
        Path path = temp.resolve("config.json");
        RunSpecJson.write(path, spec);
        assertEquals(spec, RunSpecJson.read(path));
        String json = Files.readString(path, StandardCharsets.UTF_8);
        assertTrue(json.contains("中文"));
        assertTrue(json.contains("\"where\""));
        assertFalse(json.contains("\"predicate\""));
    }

    @Test void configRejectsUnknownFieldsAtEveryLevelAndNumericCoercion() throws Exception {
        ObjectNode root = (ObjectNode) JsonSupport.MAPPER.readTree(RunSpecJson.toJson(example("mysql-to-doris.json")));
        for (String pointer : List.of("", "/reader", "/reader/connection", "/reader/connection/credentials", "/reader/connection/tls", "/reader/connection/timeouts", "/reader/source", "/reader/batch", "/writer", "/writer/options", "/execution")) {
            ObjectNode changed = root.deepCopy();
            ((ObjectNode) changed.at(pointer)).put("unsupported", true);
            assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromTree(changed), pointer);
        }
        ObjectNode changed = root.deepCopy();
        ((ObjectNode) changed.at("/execution")).put("timeout_ms", "1000");
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromTree(changed));
        ObjectNode fractional = root.deepCopy();
        ((ObjectNode) fractional.at("/execution")).put("timeout_ms", 1.5);
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromTree(fractional));
        ObjectNode coercedString = root.deepCopy();
        ((ObjectNode) coercedString.at("/reader/connection")).put("database", 123);
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromTree(coercedString));
    }

    @Test void explicitNullDoesNotRestoreNonOptionalDefaults() throws Exception {
        ObjectNode root = (ObjectNode) JsonSupport.MAPPER.readTree(RunSpecJson.toJson(example("mysql-to-doris.json")));
        for (String pointer : List.of("/reader/batch", "/reader/connection/tls", "/reader/connection/timeouts", "/writer/options", "/execution", "/reader/source/columns", "/execution/timeout_ms", "/writer/strict_mode")) {
            ObjectNode changed = root.deepCopy();
            int split = pointer.lastIndexOf('/');
            ((ObjectNode) changed.at(pointer.substring(0, split))).putNull(pointer.substring(split + 1));
            assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromTree(changed), pointer);
        }
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromJson("{\"reader\":null,\"writer\":null}"));
    }

    @Test void rejectsMalformedOrUnexpectedParameterRepresentations() throws Exception {
        for (String json : List.of("{\"type\":\"u64\",\"value\":18446744073709551615}", "{\"type\":\"null\",\"value\":null}", "{\"type\":\"bool\",\"value\":\"true\"}", "{\"type\":\"string\",\"value\":1}", "{\"type\":\"i64\",\"value\":\"9223372036854775808\"}", "{\"type\":\"string\",\"value\":\"x\",\"extra\":true}", "{\"type\":\"future\",\"value\":1}")) {
            assertThrows(JsonProcessingException.class, () -> JsonSupport.MAPPER.readValue(json, Parameter.class), json);
        }
    }

    @Test void nonDefaultFieldsHaveRustCompatibleGoldenShape() throws Exception {
        for (String filename : List.of("all-fields-mysql-to-doris.json", "all-fields-doris-to-mysql.json")) {
            Path fixture = repository().resolve("sdk/java/src/test/resources").resolve(filename);
            String json = Files.readString(fixture, StandardCharsets.UTF_8);
            RunSpec spec = RunSpecJson.fromJson(json);
            JsonNode expected = JsonSupport.MAPPER.readTree(json);
            JsonNode actual = JsonSupport.MAPPER.readTree(RunSpecJson.toJson(spec));
            assertEquals(expected, actual, filename);
            assertEquals(spec, RunSpecJson.fromTree(actual));
            assertEquals("id", spec.mapping().get(0).source());
            if (spec.reader() instanceof MysqlReader reader) {
                assertEquals("name <> ?", reader.source().predicate());
                assertEquals(Consistency.STATEMENT, reader.consistency());
                assertEquals(DorisWriteMode.UPSERT, ((DorisWriter) spec.writer()).mode());
                assertEquals(Map.of("id", "UInt64", "name", "Utf8"), reader.source().columnTypes());
            } else {
                DorisReader reader = (DorisReader) spec.reader();
                assertEquals("flight.example.test", reader.tls().serverName());
                assertEquals(2, reader.endpointParallelism());
                assertEquals(MysqlWriteMode.UPSERT, ((MysqlWriter) spec.writer()).mode());
                assertEquals(List.of("id"), ((MysqlWriter) spec.writer()).keyColumns());
                assertEquals(List.of("name"), ((MysqlWriter) spec.writer()).updateColumns());
            }
            // Kept in target for a real Rust service/CLI wire acceptance check.
            Path output = Path.of("target", "contract-configs");
            Files.createDirectories(output);
            RunSpecJson.write(output.resolve(filename), spec);
        }
    }

    @Test void defaultsAndAllWireFieldsTrackPublishedRustSchema() throws Exception {
        JsonNode schema = JsonSupport.MAPPER.readTree(Files.readString(repository().resolve("docs/connectors.schema.json"), StandardCharsets.UTF_8)).path("request_schema");
        Credentials credentials = Credentials.builder().username("sdk").password("").build();
        MysqlConnection connection = MysqlConnection.builder().database("sdk").credentials(credentials).build();
        Source source = Source.builder().table("orders").build();
        RunSpec forward = RunSpec.builder()
                .reader(MysqlReader.builder().connection(connection).source(source).build())
                .writer(DorisWriter.builder().sql(connection).feHttpUrls(List.of("http://localhost:8030")).table("orders").build()).build();
        RunSpec reverse = RunSpec.builder()
                .reader(DorisReader.builder().flightUri("grpc://localhost:8070").database("sdk").credentials(credentials).source(source).build())
                .writer(MysqlWriter.builder().connection(connection).table("orders").build()).build();
        for (RunSpec spec : List.of(forward, reverse)) {
            JsonNode actual = JsonSupport.MAPPER.readTree(RunSpecJson.toJson(spec));
            checkSchemaContract(actual, schema, schema, true, "");
        }
        for (String filename : List.of("all-fields-mysql-to-doris.json", "all-fields-doris-to-mysql.json")) {
            JsonNode actual = JsonSupport.MAPPER.readTree(RunSpecJson.toJson(RunSpecJson.read(repository().resolve("sdk/java/src/test/resources").resolve(filename))));
            checkSchemaContract(actual, schema, schema, false, "");
        }
    }

    private static void checkSchemaContract(JsonNode actual, JsonNode rawSchema, JsonNode root, boolean defaults, String path) {
        JsonNode schema = rawSchema;
        if (schema.has("$ref")) schema = root.at(schema.get("$ref").textValue().substring(1));
        if (schema.has("anyOf") && !actual.isNull()) {
            for (JsonNode candidate : schema.get("anyOf")) if (!candidate.path("type").asText().equals("null")) { schema = candidate; break; }
            if (schema.has("$ref")) schema = root.at(schema.get("$ref").textValue().substring(1));
        }
        if (schema.has("oneOf")) {
            JsonNode selected = null;
            for (JsonNode candidate : schema.get("oneOf")) if (candidate.path("properties").path("type").path("const").equals(actual.path("type"))) selected = candidate;
            assertNotNull(selected, "Unknown type at " + path);
            schema = selected;
        }
        if (actual.isObject() && schema.has("properties")) {
            JsonNode properties = schema.get("properties");
            for (JsonNode required : schema.path("required")) assertTrue(actual.has(required.textValue()), "Missing required field at " + path + "/" + required.textValue());
            var fields = actual.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                assertTrue(properties.has(field.getKey()), "Unknown wire field at " + path + "/" + field.getKey());
                checkSchemaContract(field.getValue(), properties.get(field.getKey()), root, defaults, path + "/" + field.getKey());
            }
            if (defaults) {
                var declared = properties.fields();
                while (declared.hasNext()) {
                    var property = declared.next();
                    if (property.getValue().has("default")) {
                        JsonNode expected = withoutNullObjectFields(property.getValue().get("default"));
                        JsonNode received = actual.get(property.getKey());
                        if (expected.isNull()) assertNull(received, path + "/" + property.getKey());
                        else assertEquals(expected, received, "Rust default at " + path + "/" + property.getKey());
                    }
                }
            }
        } else if (actual.isArray() && schema.has("items")) {
            for (int i = 0; i < actual.size(); i++) checkSchemaContract(actual.get(i), schema.get("items"), root, defaults, path + "/" + i);
        }
    }

    private static JsonNode withoutNullObjectFields(JsonNode node) {
        JsonNode result = node.deepCopy();
        if (result instanceof ObjectNode object) {
            java.util.ArrayList<String> remove = new java.util.ArrayList<>();
            var fields = object.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                if (field.getValue().isNull()) remove.add(field.getKey());
                else object.set(field.getKey(), withoutNullObjectFields(field.getValue()));
            }
            remove.forEach(object::remove);
        }
        return result;
    }

    @Test void rejectsDuplicateKeysAndTrailingJson() throws Exception {
        String json = RunSpecJson.toJson(example("mysql-to-doris.json"));
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromJson(json + " {}"));
        assertThrows(JsonProcessingException.class, () -> RunSpecJson.fromJson(json.replaceFirst("\"mapping\"", "\"mapping\":[],\"mapping\"")));
    }
}
