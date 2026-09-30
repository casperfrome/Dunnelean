package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Values accepted by the service. */
public enum DorisWriteMode {
    APPEND("append"),
    UPSERT("upsert");
    private final String value;
    DorisWriteMode(String value) { this.value = value; }
    @JsonValue public String value() { return value; }
    @JsonCreator public static DorisWriteMode fromValue(String value) {
        for (DorisWriteMode candidate : values()) if (candidate.value.equals(value)) return candidate;
        throw new IllegalArgumentException("Unknown DorisWriteMode: " + value);
    }
}
