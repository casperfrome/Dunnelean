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

/** Immutable MysqlReader configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = MysqlReader.Builder.class)
public record MysqlReader(
        MysqlConnection connection,
        Source source,
        Batch batch,
        Consistency consistency,
        Split split) implements ReaderConfig {
    public MysqlReader {
        connection = ConfigValues.required(connection, "connection");
        source = ConfigValues.required(source, "source");
        batch = ConfigValues.required(batch, "batch");
        consistency = ConfigValues.required(consistency, "consistency");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().connection(connection).source(source).batch(batch).consistency(consistency).split(split);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private MysqlConnection connection;
        private Source source;
        private Batch batch = Batch.builder().build();
        private Consistency consistency = Consistency.SNAPSHOT;
        private Split split;

        public Builder() {}

        public Builder connection(MysqlConnection value) { this.connection = value; return this; }
        public Builder source(Source value) { this.source = value; return this; }
        public Builder batch(Batch value) { this.batch = value; return this; }
        public Builder consistency(Consistency value) { this.consistency = value; return this; }
        public Builder split(Split value) { this.split = value; return this; }

        public MysqlReader build() { return new MysqlReader(connection, source, batch, consistency, split); }
    }
}
