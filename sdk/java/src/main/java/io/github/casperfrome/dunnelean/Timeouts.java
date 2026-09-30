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

/** Immutable Timeouts configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Timeouts.Builder.class)
public record Timeouts(
        BigInteger connectMs,
        BigInteger readMs,
        BigInteger writeMs) {
    public Timeouts {
        connectMs = ConfigValues.uint64(connectMs, "connectMs");
        readMs = ConfigValues.uint64(readMs, "readMs");
        writeMs = ConfigValues.uint64(writeMs, "writeMs");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().connectMs(connectMs).readMs(readMs).writeMs(writeMs);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private BigInteger connectMs = BigInteger.valueOf(10_000);
        private BigInteger readMs = BigInteger.valueOf(60_000);
        private BigInteger writeMs = BigInteger.valueOf(120_000);

        public Builder() {}

        public Builder connectMs(BigInteger value) { this.connectMs = value; return this; }
        public Builder connectMs(long value) { return connectMs(BigInteger.valueOf(value)); }
        public Builder readMs(BigInteger value) { this.readMs = value; return this; }
        public Builder readMs(long value) { return readMs(BigInteger.valueOf(value)); }
        public Builder writeMs(BigInteger value) { this.writeMs = value; return this; }
        public Builder writeMs(long value) { return writeMs(BigInteger.valueOf(value)); }

        public Timeouts build() { return new Timeouts(connectMs, readMs, writeMs); }
    }
}
