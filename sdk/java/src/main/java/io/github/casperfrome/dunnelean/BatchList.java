package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** Ordered durable receipts for a run. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BatchList(List<BatchReceipt> batches) {
    public BatchList {
        batches = List.copyOf(batches);
    }
}
