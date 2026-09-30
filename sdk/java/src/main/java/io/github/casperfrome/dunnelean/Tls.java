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

/** Immutable Tls configuration. Omitted builder fields use the service defaults. */
@JsonDeserialize(builder = Tls.Builder.class)
public record Tls(
        boolean enabled,
        String caCertificate,
        String clientCertificate,
        String clientKey,
        String serverName) {
    public Tls {
    }

    /** Creates a builder. */
    public static Builder builder() { return new Builder(); }

    /** Creates a builder initialized with this configuration. */
    public Builder toBuilder() {
        return new Builder().enabled(enabled).caCertificate(caCertificate).clientCertificate(clientCertificate).clientKey(clientKey).serverName(serverName);
    }

    /** Builder for immutable configuration. */
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private boolean enabled = false;
        private String caCertificate;
        private String clientCertificate;
        private String clientKey;
        private String serverName;

        public Builder() {}

        public Builder enabled(boolean value) { this.enabled = value; return this; }
        public Builder caCertificate(String value) { this.caCertificate = value; return this; }
        public Builder clientCertificate(String value) { this.clientCertificate = value; return this; }
        public Builder clientKey(String value) { this.clientKey = value; return this; }
        public Builder serverName(String value) { this.serverName = value; return this; }

        public Tls build() { return new Tls(enabled, caCertificate, clientCertificate, clientKey, serverName); }
    }
}
