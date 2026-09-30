package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Objects;

/** Connector descriptions, current request JSON Schema, and service limits. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Connectors(List<JsonNode> connectors, JsonNode requestSchema, JsonNode limits) {
    public Connectors {
        connectors = copies(connectors);
        requestSchema = Objects.requireNonNull(requestSchema, "requestSchema").deepCopy();
        limits = Objects.requireNonNull(limits, "limits").deepCopy();
    }

    @Override
    public List<JsonNode> connectors() {
        return copies(connectors);
    }

    @Override
    public JsonNode requestSchema() {
        return requestSchema.deepCopy();
    }

    @Override
    public JsonNode limits() {
        return limits.deepCopy();
    }

    private static List<JsonNode> copies(List<JsonNode> values) {
        return values.stream().map(JsonNode::<JsonNode>deepCopy).toList();
    }
}
