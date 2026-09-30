package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Values accepted by the service. */
public enum Consistency {
    SNAPSHOT("snapshot"),
    STATEMENT("statement");
    private final String value;
    Consistency(String value) { this.value = value; }
    @JsonValue public String value() { return value; }
    @JsonCreator public static Consistency fromValue(String value) {
        for (Consistency candidate : values()) if (candidate.value.equals(value)) return candidate;
        throw new IllegalArgumentException("Unknown Consistency: " + value);
    }
}
