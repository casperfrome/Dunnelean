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

/** Immutable Execution configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Execution.Builder.class)
public record Execution(
        BigInteger timeoutMs,
        BigInteger queueCapacity,
        BigInteger memoryBytes,
        BigInteger maxRowBytes,
        BigInteger rowsPerSecond,
        BigInteger bytesPerSecond) {
    public Execution {
        timeoutMs = ConfigValues.uint64(timeoutMs, "timeoutMs");
        queueCapacity = ConfigValues.uint64(queueCapacity, "queueCapacity");
        memoryBytes = ConfigValues.uint64(memoryBytes, "memoryBytes");
        maxRowBytes = ConfigValues.uint64(maxRowBytes, "maxRowBytes");
        if (rowsPerSecond != null) rowsPerSecond = ConfigValues.uint64(rowsPerSecond, "rowsPerSecond");
        if (bytesPerSecond != null) bytesPerSecond = ConfigValues.uint64(bytesPerSecond, "bytesPerSecond");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().timeoutMs(timeoutMs).queueCapacity(queueCapacity).memoryBytes(memoryBytes).maxRowBytes(maxRowBytes).rowsPerSecond(rowsPerSecond).bytesPerSecond(bytesPerSecond);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private BigInteger timeoutMs = BigInteger.valueOf(3_600_000);
        private BigInteger queueCapacity = BigInteger.valueOf(4);
        private BigInteger memoryBytes = BigInteger.valueOf(256L * 1024 * 1024);
        private BigInteger maxRowBytes = BigInteger.valueOf(24L * 1024 * 1024);
        private BigInteger rowsPerSecond;
        private BigInteger bytesPerSecond;

        public Builder() {}

        public Builder timeoutMs(BigInteger value) { this.timeoutMs = value; return this; }
        public Builder timeoutMs(long value) { return timeoutMs(BigInteger.valueOf(value)); }
        public Builder queueCapacity(BigInteger value) { this.queueCapacity = value; return this; }
        public Builder queueCapacity(long value) { return queueCapacity(BigInteger.valueOf(value)); }
        public Builder memoryBytes(BigInteger value) { this.memoryBytes = value; return this; }
        public Builder memoryBytes(long value) { return memoryBytes(BigInteger.valueOf(value)); }
        public Builder maxRowBytes(BigInteger value) { this.maxRowBytes = value; return this; }
        public Builder maxRowBytes(long value) { return maxRowBytes(BigInteger.valueOf(value)); }
        public Builder rowsPerSecond(BigInteger value) { this.rowsPerSecond = value; return this; }
        public Builder rowsPerSecond(long value) { return rowsPerSecond(BigInteger.valueOf(value)); }
        public Builder bytesPerSecond(BigInteger value) { this.bytesPerSecond = value; return this; }
        public Builder bytesPerSecond(long value) { return bytesPerSecond(BigInteger.valueOf(value)); }

        public Execution build() { return new Execution(timeoutMs, queueCapacity, memoryBytes, maxRowBytes, rowsPerSecond, bytesPerSecond); }
    }
}
