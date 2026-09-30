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

/** Immutable WriterOptions configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = WriterOptions.Builder.class)
public record WriterOptions(
        Batch batch,
        int parallelism,
        BigInteger retryAttempts,
        BigInteger retryBackoffMs,
        List<String> preSql,
        List<String> postSql) {
    public WriterOptions {
        batch = ConfigValues.required(batch, "batch");
        ConfigValues.range(parallelism, 1, 32, "parallelism");
        retryAttempts = ConfigValues.uint64(retryAttempts, "retryAttempts");
        retryBackoffMs = ConfigValues.uint64(retryBackoffMs, "retryBackoffMs");
        preSql = List.copyOf(ConfigValues.required(preSql, "preSql"));
        postSql = List.copyOf(ConfigValues.required(postSql, "postSql"));
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().batch(batch).parallelism(parallelism).retryAttempts(retryAttempts).retryBackoffMs(retryBackoffMs).preSql(preSql).postSql(postSql);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private Batch batch = Batch.builder().build();
        private int parallelism = 1;
        private BigInteger retryAttempts = BigInteger.valueOf(2);
        private BigInteger retryBackoffMs = BigInteger.valueOf(250);
        private List<String> preSql = List.of();
        private List<String> postSql = List.of();

        public Builder() {}

        public Builder batch(Batch value) { this.batch = value; return this; }
        public Builder parallelism(int value) { this.parallelism = value; return this; }
        public Builder retryAttempts(BigInteger value) { this.retryAttempts = value; return this; }
        public Builder retryAttempts(long value) { return retryAttempts(BigInteger.valueOf(value)); }
        public Builder retryBackoffMs(BigInteger value) { this.retryBackoffMs = value; return this; }
        public Builder retryBackoffMs(long value) { return retryBackoffMs(BigInteger.valueOf(value)); }
        public Builder preSql(List<String> value) { this.preSql = value; return this; }
        public Builder postSql(List<String> value) { this.postSql = value; return this; }

        public WriterOptions build() { return new WriterOptions(batch, parallelism, retryAttempts, retryBackoffMs, preSql, postSql); }
    }
}
