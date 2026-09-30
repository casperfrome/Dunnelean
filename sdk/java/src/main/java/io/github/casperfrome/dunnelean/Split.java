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

/** Immutable Split configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Split.Builder.class)
public record Split(
        String column,
        String lowerBound,
        String upperBound,
        int partitions,
        int parallelism) {
    public Split {
        column = ConfigValues.required(column, "column");
        lowerBound = ConfigValues.bound(lowerBound, "lowerBound");
        upperBound = ConfigValues.bound(upperBound, "upperBound");
        ConfigValues.range(partitions, 1, 1024, "partitions");
        ConfigValues.range(parallelism, 1, 32, "parallelism");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().column(column).lowerBound(lowerBound).upperBound(upperBound).partitions(partitions).parallelism(parallelism);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String column;
        private String lowerBound;
        private String upperBound;
        private int partitions;
        private int parallelism = 1;

        public Builder() {}

        public Builder column(String value) { this.column = value; return this; }
        public Builder lowerBound(String value) { this.lowerBound = value; return this; }
        public Builder lowerBound(long value) { return lowerBound(Long.toString(value)); }
        public Builder lowerBound(BigInteger value) { return lowerBound(ConfigValues.required(value, "lowerBound").toString()); }
        public Builder upperBound(String value) { this.upperBound = value; return this; }
        public Builder upperBound(long value) { return upperBound(Long.toString(value)); }
        public Builder upperBound(BigInteger value) { return upperBound(ConfigValues.required(value, "upperBound").toString()); }
        public Builder partitions(int value) { this.partitions = value; return this; }
        public Builder parallelism(int value) { this.parallelism = value; return this; }

        public Split build() { return new Split(column, lowerBound, upperBound, partitions, parallelism); }
    }
}
