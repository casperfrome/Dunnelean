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

/** Immutable Source configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Source.Builder.class)
public record Source(
        String table,
        List<String> columns,
        @JsonProperty("where") String predicate,
        String query,
        List<Parameter> params,
        Map<String, String> columnTypes) {
    public Source {
        columns = List.copyOf(ConfigValues.required(columns, "columns"));
        params = List.copyOf(ConfigValues.required(params, "params"));
        columnTypes = Map.copyOf(ConfigValues.required(columnTypes, "columnTypes"));
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().table(table).columns(columns).predicate(predicate).query(query).params(params).columnTypes(columnTypes);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String table;
        private List<String> columns = List.of();
        @JsonProperty("where")
        private String predicate;
        private String query;
        private List<Parameter> params = List.of();
        private Map<String, String> columnTypes = Map.of();

        public Builder() {}

        public Builder table(String value) { this.table = value; return this; }
        public Builder columns(List<String> value) { this.columns = value; return this; }
        @JsonProperty("where")
        public Builder predicate(String value) { this.predicate = value; return this; }
        public Builder query(String value) { this.query = value; return this; }
        public Builder params(List<Parameter> value) { this.params = value; return this; }
        public Builder columnTypes(Map<String, String> value) { this.columnTypes = value; return this; }

        public Source build() { return new Source(table, columns, predicate, query, params, columnTypes); }
    }
}
