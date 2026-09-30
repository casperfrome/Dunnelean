package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** A source or target column; type is the service's Arrow type description. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Field(String name, String type, @JsonProperty(value = "nullable", required = true) boolean nullable) {
    public Field {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
    }
}
