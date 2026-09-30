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

/** Immutable Batch configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Batch.Builder.class)
public record Batch(
        BigInteger rows,
        BigInteger bytes) {
    public Batch {
        rows = ConfigValues.uint64(rows, "rows");
        bytes = ConfigValues.uint64(bytes, "bytes");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().rows(rows).bytes(bytes);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private BigInteger rows = BigInteger.valueOf(10_000);
        private BigInteger bytes = BigInteger.valueOf(16L * 1024 * 1024);

        public Builder() {}

        public Builder rows(BigInteger value) { this.rows = value; return this; }
        public Builder rows(long value) { return rows(BigInteger.valueOf(value)); }
        public Builder bytes(BigInteger value) { this.bytes = value; return this; }
        public Builder bytes(long value) { return bytes(BigInteger.valueOf(value)); }

        public Batch build() { return new Batch(rows, bytes); }
    }
}
