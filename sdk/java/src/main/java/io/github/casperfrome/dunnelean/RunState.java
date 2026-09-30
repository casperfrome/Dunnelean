package io.github.casperfrome.dunnelean;

import java.util.Optional;

/** Known service run states. Future states remain available as {@link Run#state()}. */
public enum RunState {
    QUEUED(false), RUNNING(false), CANCELLING(false),
    SUCCEEDED(true), FAILED(true), CANCELLED(true), INTERRUPTED(true);

    private final boolean terminal;

    RunState(boolean terminal) {
        this.terminal = terminal;
    }

    /** Whether the service has finished this run. */
    public boolean terminal() {
        return terminal;
    }

    /** Resolve an exact wire value without failing on unknown future states. */
    public static Optional<RunState> from(String value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(value));
        } catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }
}
