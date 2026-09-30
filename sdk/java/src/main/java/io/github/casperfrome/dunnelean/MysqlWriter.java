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

/** Immutable MysqlWriter configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = MysqlWriter.Builder.class)
public record MysqlWriter(
        MysqlConnection connection,
        String table,
        MysqlWriteMode mode,
        List<String> keyColumns,
        List<String> updateColumns,
        WriterOptions options) implements WriterConfig {
    public MysqlWriter {
        connection = ConfigValues.required(connection, "connection");
        table = ConfigValues.required(table, "table");
        mode = ConfigValues.required(mode, "mode");
        keyColumns = List.copyOf(ConfigValues.required(keyColumns, "keyColumns"));
        updateColumns = List.copyOf(ConfigValues.required(updateColumns, "updateColumns"));
        options = ConfigValues.required(options, "options");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().connection(connection).table(table).mode(mode).keyColumns(keyColumns).updateColumns(updateColumns).options(options);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private MysqlConnection connection;
        private String table;
        private MysqlWriteMode mode = MysqlWriteMode.INSERT;
        private List<String> keyColumns = List.of();
        private List<String> updateColumns = List.of();
        private WriterOptions options = WriterOptions.builder().build();

        public Builder() {}

        public Builder connection(MysqlConnection value) { this.connection = value; return this; }
        public Builder table(String value) { this.table = value; return this; }
        public Builder mode(MysqlWriteMode value) { this.mode = value; return this; }
        public Builder keyColumns(List<String> value) { this.keyColumns = value; return this; }
        public Builder updateColumns(List<String> value) { this.updateColumns = value; return this; }
        public Builder options(WriterOptions value) { this.options = value; return this; }

        public MysqlWriter build() { return new MysqlWriter(connection, table, mode, keyColumns, updateColumns, options); }
    }
}
