package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/** Readiness result; an unready service responds with an API error instead. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Ready(String status) {
    public Ready {
        Objects.requireNonNull(status, "status");
    }
}
