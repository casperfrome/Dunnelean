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

/** Immutable MysqlConnection configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = MysqlConnection.Builder.class)
public record MysqlConnection(
        String host,
        int port,
        String database,
        Credentials credentials,
        Tls tls,
        Timeouts timeouts,
        String charset,
        String timeZone,
        Map<String, String> sessionVariables,
        BigInteger maxAllowedPacketBytes) {
    public MysqlConnection {
        host = ConfigValues.required(host, "host");
        ConfigValues.range(port, 0, 65535, "port");
        database = ConfigValues.required(database, "database");
        credentials = ConfigValues.required(credentials, "credentials");
        tls = ConfigValues.required(tls, "tls");
        timeouts = ConfigValues.required(timeouts, "timeouts");
        charset = ConfigValues.required(charset, "charset");
        timeZone = ConfigValues.required(timeZone, "timeZone");
        sessionVariables = Map.copyOf(ConfigValues.required(sessionVariables, "sessionVariables"));
        maxAllowedPacketBytes = ConfigValues.uint64(maxAllowedPacketBytes, "maxAllowedPacketBytes");
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().host(host).port(port).database(database).credentials(credentials).tls(tls).timeouts(timeouts).charset(charset).timeZone(timeZone).sessionVariables(sessionVariables).maxAllowedPacketBytes(maxAllowedPacketBytes);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String host = "127.0.0.1";
        private int port = 3306;
        private String database;
        private Credentials credentials;
        private Tls tls = Tls.builder().build();
        private Timeouts timeouts = Timeouts.builder().build();
        private String charset = "utf8mb4";
        private String timeZone = "+00:00";
        private Map<String, String> sessionVariables = Map.of();
        private BigInteger maxAllowedPacketBytes = BigInteger.valueOf(64L * 1024 * 1024);

        public Builder() {}

        public Builder host(String value) { this.host = value; return this; }
        public Builder port(int value) { this.port = value; return this; }
        public Builder database(String value) { this.database = value; return this; }
        public Builder credentials(Credentials value) { this.credentials = value; return this; }
        public Builder tls(Tls value) { this.tls = value; return this; }
        public Builder timeouts(Timeouts value) { this.timeouts = value; return this; }
        public Builder charset(String value) { this.charset = value; return this; }
        public Builder timeZone(String value) { this.timeZone = value; return this; }
        public Builder sessionVariables(Map<String, String> value) { this.sessionVariables = value; return this; }
        public Builder maxAllowedPacketBytes(BigInteger value) { this.maxAllowedPacketBytes = value; return this; }
        public Builder maxAllowedPacketBytes(long value) { return maxAllowedPacketBytes(BigInteger.valueOf(value)); }

        public MysqlConnection build() { return new MysqlConnection(host, port, database, credentials, tls, timeouts, charset, timeZone, sessionVariables, maxAllowedPacketBytes); }
    }
}
