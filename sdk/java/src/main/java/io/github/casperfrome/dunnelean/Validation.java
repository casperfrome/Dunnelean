package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;

/** Validated source and target schemas and the service's commit semantics. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Validation(@JsonProperty(value = "valid", required = true) boolean valid,
                         List<Field> sourceSchema, List<Field> targetSchema, String semantics) {
    public Validation {
        sourceSchema = List.copyOf(sourceSchema);
        targetSchema = List.copyOf(targetSchema);
        Objects.requireNonNull(semantics, "semantics");
    }
}
