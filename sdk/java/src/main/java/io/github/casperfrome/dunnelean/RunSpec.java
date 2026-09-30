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

/** Immutable RunSpec configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = RunSpec.Builder.class)
public record RunSpec(
        String requestId,
        ReaderConfig reader,
        WriterConfig writer,
        List<Mapping> mapping,
        Execution execution) {
    public RunSpec {
        reader = ConfigValues.required(reader, "reader");
        writer = ConfigValues.required(writer, "writer");
        mapping = List.copyOf(ConfigValues.required(mapping, "mapping"));
        execution = ConfigValues.required(execution, "execution");
        ConfigValues.requestId(requestId);
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().requestId(requestId).reader(reader).writer(writer).mapping(mapping).execution(execution);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String requestId;
        private ReaderConfig reader;
        private WriterConfig writer;
        private List<Mapping> mapping = List.of();
        private Execution execution = Execution.builder().build();

        public Builder() {}

        public Builder requestId(String value) { this.requestId = value; return this; }
        public Builder reader(ReaderConfig value) { this.reader = value; return this; }
        public Builder writer(WriterConfig value) { this.writer = value; return this; }
        public Builder mapping(List<Mapping> value) { this.mapping = value; return this; }
        public Builder execution(Execution value) { this.execution = value; return this; }

        public RunSpec build() { return new RunSpec(requestId, reader, writer, mapping, execution); }
    }
}
