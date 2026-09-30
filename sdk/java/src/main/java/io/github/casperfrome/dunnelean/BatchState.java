package io.github.casperfrome.dunnelean;

import java.util.Optional;

/** Known batch receipt states; UNKNOWN means the commit outcome is uncertain. */
public enum BatchState {
    INTENT, CONFIRMED, NOT_COMMITTED, UNKNOWN;

    /** Resolve an exact wire value without rejecting future states. */
    public static Optional<BatchState> from(String value) {
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
