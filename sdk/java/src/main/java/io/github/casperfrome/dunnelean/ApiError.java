package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** The service's error payload, also attached to a failed run. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ApiError(String code, String message,
                       @JsonProperty(value = "commit_unknown", required = true) boolean commitUnknown,
                       @JsonProperty(value = "retryable", required = true) boolean retryable) {
    public ApiError {
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(message, "message");
    }
}
