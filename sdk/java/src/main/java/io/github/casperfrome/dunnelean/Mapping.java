package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/** Immutable Mapping configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Mapping.Builder.class)
public record Mapping(
        String source,
        String target) {
    public Mapping {
        source = ConfigValues.required(source, "source");
        target = ConfigValues.required(target, "target");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().source(source).target(target);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String source;
        private String target;

        public Builder() {}

        public Builder source(String value) { this.source = value; return this; }
        public Builder target(String value) { this.target = value; return this; }

        public Mapping build() { return new Mapping(source, target); }
    }
}
