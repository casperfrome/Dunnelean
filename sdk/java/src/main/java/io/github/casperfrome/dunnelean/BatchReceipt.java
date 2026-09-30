package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigInteger;
import java.util.Optional;
import java.util.Objects;

/** Receipt metadata; detail is the serialized database receipt or error, not batch data. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BatchReceipt(BigInteger batchId, String state, BigInteger rows, BigInteger bytes,
                           String label, String detail) {
    public BatchReceipt {
        ConfigValues.uint64(batchId, "batchId");
        Objects.requireNonNull(state, "state");
        ConfigValues.uint64(rows, "rows");
        ConfigValues.uint64(bytes, "bytes");
    }

    /** Resolve known receipt states while retaining unknown values in {@link #state()}. */
    @JsonIgnore
    public Optional<BatchState> knownState() {
        return BatchState.from(state);
    }
}
