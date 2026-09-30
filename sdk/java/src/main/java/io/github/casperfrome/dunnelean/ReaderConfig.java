package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/** Typed connector configuration, encoded with the service's type discriminator. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = MysqlReader.class, name = "mysql"),
    @JsonSubTypes.Type(value = DorisReader.class, name = "doris")
})
public sealed interface ReaderConfig permits MysqlReader, DorisReader {}
