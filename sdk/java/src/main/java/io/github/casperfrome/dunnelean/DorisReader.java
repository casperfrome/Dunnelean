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

/** Immutable DorisReader configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = DorisReader.Builder.class)
public record DorisReader(
        String flightUri,
        String database,
        Credentials credentials,
        Tls tls,
        Timeouts timeouts,
        Map<String, String> sessionVariables,
        Map<String, String> endpointMap,
        Source source,
        Batch batch,
        int endpointParallelism,
        BigInteger maxGrpcMessageBytes) implements ReaderConfig {
    public DorisReader {
        flightUri = ConfigValues.required(flightUri, "flightUri");
        database = ConfigValues.required(database, "database");
        credentials = ConfigValues.required(credentials, "credentials");
        tls = ConfigValues.required(tls, "tls");
        timeouts = ConfigValues.required(timeouts, "timeouts");
        sessionVariables = Map.copyOf(ConfigValues.required(sessionVariables, "sessionVariables"));
        endpointMap = Map.copyOf(ConfigValues.required(endpointMap, "endpointMap"));
        source = ConfigValues.required(source, "source");
        batch = ConfigValues.required(batch, "batch");
        ConfigValues.range(endpointParallelism, 1, 32, "endpointParallelism");
        maxGrpcMessageBytes = ConfigValues.uint64(maxGrpcMessageBytes, "maxGrpcMessageBytes");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().flightUri(flightUri).database(database).credentials(credentials).tls(tls).timeouts(timeouts).sessionVariables(sessionVariables).endpointMap(endpointMap).source(source).batch(batch).endpointParallelism(endpointParallelism).maxGrpcMessageBytes(maxGrpcMessageBytes);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String flightUri;
        private String database;
        private Credentials credentials;
        private Tls tls = Tls.builder().build();
        private Timeouts timeouts = Timeouts.builder().build();
        private Map<String, String> sessionVariables = Map.of();
        private Map<String, String> endpointMap = Map.of();
        private Source source;
        private Batch batch = Batch.builder().build();
        private int endpointParallelism = 1;
        private BigInteger maxGrpcMessageBytes = BigInteger.valueOf(64L * 1024 * 1024);

        public Builder() {}

        public Builder flightUri(String value) { this.flightUri = value; return this; }
        public Builder database(String value) { this.database = value; return this; }
        public Builder credentials(Credentials value) { this.credentials = value; return this; }
        public Builder tls(Tls value) { this.tls = value; return this; }
        public Builder timeouts(Timeouts value) { this.timeouts = value; return this; }
        public Builder sessionVariables(Map<String, String> value) { this.sessionVariables = value; return this; }
        public Builder endpointMap(Map<String, String> value) { this.endpointMap = value; return this; }
        public Builder source(Source value) { this.source = value; return this; }
        public Builder batch(Batch value) { this.batch = value; return this; }
        public Builder endpointParallelism(int value) { this.endpointParallelism = value; return this; }
        public Builder maxGrpcMessageBytes(BigInteger value) { this.maxGrpcMessageBytes = value; return this; }
        public Builder maxGrpcMessageBytes(long value) { return maxGrpcMessageBytes(BigInteger.valueOf(value)); }

        public DorisReader build() { return new DorisReader(flightUri, database, credentials, tls, timeouts, sessionVariables, endpointMap, source, batch, endpointParallelism, maxGrpcMessageBytes); }
    }
}
