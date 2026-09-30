package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Idempotency and cancellation status; run is null for a cancellation recorded before submission. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RequestStatus(String requestId,
                            @JsonProperty(value = "cancel_requested", required = true) boolean cancelRequested,
                            Run run) {
    public RequestStatus {
        Objects.requireNonNull(requestId, "requestId");
    }
}
