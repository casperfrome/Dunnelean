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

/** Immutable DorisWriter configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = DorisWriter.Builder.class)
public record DorisWriter(
        MysqlConnection sql,
        List<String> feHttpUrls,
        List<String> beHttpUrls,
        Map<String, String> endpointMap,
        Tls httpTls,
        String table,
        DorisWriteMode mode,
        WriterOptions options,
        String labelPrefix,
        BigInteger loadTimeoutSeconds,
        boolean strictMode,
        double maxFilterRatio,
        String timeZone,
        List<String> partitions,
        BigInteger execMemLimit,
        Map<String, String> headers) implements WriterConfig {
    public DorisWriter {
        sql = ConfigValues.required(sql, "sql");
        feHttpUrls = List.copyOf(ConfigValues.required(feHttpUrls, "feHttpUrls"));
        beHttpUrls = List.copyOf(ConfigValues.required(beHttpUrls, "beHttpUrls"));
        endpointMap = Map.copyOf(ConfigValues.required(endpointMap, "endpointMap"));
        httpTls = ConfigValues.required(httpTls, "httpTls");
        table = ConfigValues.required(table, "table");
        mode = ConfigValues.required(mode, "mode");
        options = ConfigValues.required(options, "options");
        labelPrefix = ConfigValues.required(labelPrefix, "labelPrefix");
        loadTimeoutSeconds = ConfigValues.uint64(loadTimeoutSeconds, "loadTimeoutSeconds");
        if (!Double.isFinite(maxFilterRatio) || maxFilterRatio < 0 || maxFilterRatio > 1) throw new IllegalArgumentException("maxFilterRatio must be 0..1");
        timeZone = ConfigValues.required(timeZone, "timeZone");
        partitions = List.copyOf(ConfigValues.required(partitions, "partitions"));
        execMemLimit = ConfigValues.uint64(execMemLimit, "execMemLimit");
        headers = Map.copyOf(ConfigValues.required(headers, "headers"));
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().sql(sql).feHttpUrls(feHttpUrls).beHttpUrls(beHttpUrls).endpointMap(endpointMap).httpTls(httpTls).table(table).mode(mode).options(options).labelPrefix(labelPrefix).loadTimeoutSeconds(loadTimeoutSeconds).strictMode(strictMode).maxFilterRatio(maxFilterRatio).timeZone(timeZone).partitions(partitions).execMemLimit(execMemLimit).headers(headers);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private MysqlConnection sql;
        private List<String> feHttpUrls;
        private List<String> beHttpUrls = List.of();
        private Map<String, String> endpointMap = Map.of();
        private Tls httpTls = Tls.builder().build();
        private String table;
        private DorisWriteMode mode = DorisWriteMode.APPEND;
        private WriterOptions options = WriterOptions.builder().build();
        private String labelPrefix = "dunnelean";
        private BigInteger loadTimeoutSeconds = BigInteger.valueOf(120);
        private boolean strictMode = true;
        private double maxFilterRatio = 0.0;
        private String timeZone = "+00:00";
        private List<String> partitions = List.of();
        private BigInteger execMemLimit = BigInteger.valueOf(2L * 1024 * 1024 * 1024);
        private Map<String, String> headers = Map.of();

        public Builder() {}

        public Builder sql(MysqlConnection value) { this.sql = value; return this; }
        public Builder feHttpUrls(List<String> value) { this.feHttpUrls = value; return this; }
        public Builder beHttpUrls(List<String> value) { this.beHttpUrls = value; return this; }
        public Builder endpointMap(Map<String, String> value) { this.endpointMap = value; return this; }
        public Builder httpTls(Tls value) { this.httpTls = value; return this; }
        public Builder table(String value) { this.table = value; return this; }
        public Builder mode(DorisWriteMode value) { this.mode = value; return this; }
        public Builder options(WriterOptions value) { this.options = value; return this; }
        public Builder labelPrefix(String value) { this.labelPrefix = value; return this; }
        public Builder loadTimeoutSeconds(BigInteger value) { this.loadTimeoutSeconds = value; return this; }
        public Builder loadTimeoutSeconds(long value) { return loadTimeoutSeconds(BigInteger.valueOf(value)); }
        public Builder strictMode(boolean value) { this.strictMode = value; return this; }
        public Builder maxFilterRatio(double value) { this.maxFilterRatio = value; return this; }
        public Builder timeZone(String value) { this.timeZone = value; return this; }
        public Builder partitions(List<String> value) { this.partitions = value; return this; }
        public Builder execMemLimit(BigInteger value) { this.execMemLimit = value; return this; }
        public Builder execMemLimit(long value) { return execMemLimit(BigInteger.valueOf(value)); }
        public Builder headers(Map<String, String> value) { this.headers = value; return this; }

        public DorisWriter build() { return new DorisWriter(sql, feHttpUrls, beHttpUrls, endpointMap, httpTls, table, mode, options, labelPrefix, loadTimeoutSeconds, strictMode, maxFilterRatio, timeZone, partitions, execMemLimit, headers); }
    }
}
