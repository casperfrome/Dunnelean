package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Values accepted by the service. */
public enum MysqlWriteMode {
    INSERT("insert"),
    UPSERT("upsert");
    private final String value;
    MysqlWriteMode(String value) { this.value = value; }
    @JsonValue public String value() { return value; }
    @JsonCreator public static MysqlWriteMode fromValue(String value) {
        for (MysqlWriteMode candidate : values()) if (candidate.value.equals(value)) return candidate;
        throw new IllegalArgumentException("Unknown MysqlWriteMode: " + value);
    }
}
