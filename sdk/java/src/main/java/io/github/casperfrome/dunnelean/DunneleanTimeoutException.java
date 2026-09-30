package io.github.casperfrome.dunnelean;

import java.time.Duration;
import java.util.Objects;

/** Completion polling expired; the remote run continues until explicitly cancelled. */
public final class DunneleanTimeoutException extends DunneleanException {
    private static final long serialVersionUID = 1L;
    private final String runId;
    private final Duration timeout;

    public DunneleanTimeoutException(String runId, Duration timeout) {
        super("Waiting for Dunnelean run " + runId + " exceeded " + timeout);
        this.runId = Objects.requireNonNull(runId, "runId");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    public String runId() {
        return runId;
    }

    public Duration timeout() {
        return timeout;
    }
}
