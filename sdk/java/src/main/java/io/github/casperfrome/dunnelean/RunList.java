package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** A page of persisted runs; pagination is supplied on the corresponding request. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RunList(List<Run> runs) {
    public RunList {
        runs = List.copyOf(runs);
    }
}
