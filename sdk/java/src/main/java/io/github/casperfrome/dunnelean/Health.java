package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/** Service identity and durable state-store identity returned by the health endpoint. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Health(String status, String service, String version, String stateStoreId) {
    public Health {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(service, "service");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(stateStoreId, "stateStoreId");
    }
}
