package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Optional;
import java.util.Objects;

/** A persisted run, with exact unsigned counters and the service's redacted configuration. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Run(
        String runId,
        String requestId,
        String state,
        String stage,
        Instant createdAt,
        Instant updatedAt,
        BigInteger rowsRead,
        BigInteger bytesRead,
        BigInteger rowsSubmitted,
        BigInteger rowsCommitted,
        BigInteger rowsFiltered,
        BigInteger serverAffectedRows,
        BigInteger batchesCommitted,
        @JsonProperty(value = "partial_write", required = true) boolean partialWrite,
        @JsonProperty(value = "commit_unknown", required = true) boolean commitUnknown,
        ApiError error,
        JsonNode config) {

    public Run {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        ConfigValues.uint64(rowsRead, "rowsRead");
        ConfigValues.uint64(bytesRead, "bytesRead");
        ConfigValues.uint64(rowsSubmitted, "rowsSubmitted");
        ConfigValues.uint64(rowsCommitted, "rowsCommitted");
        ConfigValues.uint64(rowsFiltered, "rowsFiltered");
        ConfigValues.uint64(serverAffectedRows, "serverAffectedRows");
        ConfigValues.uint64(batchesCommitted, "batchesCommitted");
        config = Objects.requireNonNull(config, "config").deepCopy();
    }

    /** Return a defensive copy of the configuration, which the service already redacts. */
    @Override
    public JsonNode config() {
        return config.deepCopy();
    }

    /** Resolve known states while preserving the original wire state in {@link #state()}. */
    @JsonIgnore
    public Optional<RunState> knownState() {
        return RunState.from(state);
    }

    /** Unknown future states are considered nonterminal so polling remains bounded by its deadline. */
    @JsonIgnore
    public boolean terminal() {
        return knownState().map(RunState::terminal).orElse(false);
    }
}
