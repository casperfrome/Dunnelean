package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** UTF-8 conversion between existing service JSON files and typed configuration. */
public final class RunSpecJson {
    private RunSpecJson() {}
    public static RunSpec fromJson(String json) throws JsonProcessingException {
        return ConfigValues.required(JsonSupport.MAPPER.readValue(ConfigValues.required(json, "json"), RunSpec.class), "runSpec");
    }
    public static RunSpec read(Path path) throws IOException {
        return fromJson(Files.readString(ConfigValues.required(path, "path"), StandardCharsets.UTF_8));
    }
    public static String toJson(RunSpec spec) throws JsonProcessingException {
        return JsonSupport.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(ConfigValues.required(spec, "spec"));
    }
    public static void write(Path path, RunSpec spec) throws IOException {
        Files.writeString(ConfigValues.required(path, "path"), toJson(spec), StandardCharsets.UTF_8);
    }
    public static RunSpec fromTree(JsonNode tree) throws JsonProcessingException {
        return ConfigValues.required(JsonSupport.MAPPER.treeToValue(ConfigValues.required(tree, "tree"), RunSpec.class), "runSpec");
    }
}
