package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Set;

/** Typed bound SQL value. Integer, decimal and binary wire values remain strings. */
@JsonSerialize(using = Parameter.Serializer.class)
@JsonDeserialize(using = Parameter.Deserializer.class)
public sealed interface Parameter permits Parameter.Null, Parameter.StringValue, Parameter.I64, Parameter.U64, Parameter.Decimal, Parameter.F64, Parameter.Bool, Parameter.Binary {
    String type();
    static Null nullValue() { return new Null(); }
    static StringValue string(String value) { return new StringValue(value); }
    static I64 i64(long value) { return new I64(Long.toString(value)); }
    static I64 i64(String value) { return new I64(value); }
    static I64 i64(BigInteger value) { return new I64(ConfigValues.required(value, "value").toString()); }
    static U64 u64(long value) { return new U64(Long.toString(value)); }
    static U64 u64(String value) { return new U64(value); }
    static U64 u64(BigInteger value) { return new U64(ConfigValues.required(value, "value").toString()); }
    static Decimal decimal(String value) { return new Decimal(value); }
    static Decimal decimal(BigDecimal value) { return new Decimal(ConfigValues.required(value, "value").toPlainString()); }
    static F64 f64(double value) { return new F64(value); }
    static Bool bool(boolean value) { return new Bool(value); }
    static Binary binary(String value) { return new Binary(value); }
    static Binary binary(byte[] value) { return new Binary(Base64.getEncoder().encodeToString(ConfigValues.required(value, "value"))); }

    record Null() implements Parameter { @Override public String type() { return "null"; } }
    record StringValue(String value) implements Parameter {
        public StringValue { ConfigValues.required(value, "value"); }
        @Override public String type() { return "string"; }
    }
    record I64(String value) implements Parameter {
        public I64 {
            BigInteger integer = ConfigValues.integer(value, "i64 value");
            if (integer.compareTo(ConfigValues.INT64_MIN) < 0 || integer.compareTo(ConfigValues.INT64_MAX) > 0) throw new IllegalArgumentException("i64 value is outside signed 64-bit range");
        }
        @Override public String type() { return "i64"; }
    }
    record U64(String value) implements Parameter {
        public U64 { ConfigValues.uint64(ConfigValues.integer(value, "u64 value"), "u64 value"); }
        @Override public String type() { return "u64"; }
    }
    record Decimal(String value) implements Parameter {
        public Decimal { new BigDecimal(ConfigValues.required(value, "value")); }
        @Override public String type() { return "decimal"; }
    }
    record F64(double value) implements Parameter {
        public F64 { if (!Double.isFinite(value)) throw new IllegalArgumentException("f64 value must be finite"); }
        @Override public String type() { return "f64"; }
    }
    record Bool(boolean value) implements Parameter { @Override public String type() { return "bool"; } }
    record Binary(String value) implements Parameter {
        public Binary {
            ConfigValues.required(value, "value");
            byte[] decoded = Base64.getDecoder().decode(value);
            if (!Base64.getEncoder().encodeToString(decoded).equals(value)) throw new IllegalArgumentException("binary value must use canonical standard base64");
        }
        @Override public String type() { return "binary"; }
    }

    /** Emits the service's adjacent type/value tagged representation. */
    final class Serializer extends JsonSerializer<Parameter> {
        @Override public void serialize(Parameter parameter, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeStartObject();
            generator.writeStringField("type", parameter.type());
            if (parameter instanceof StringValue value) generator.writeStringField("value", value.value());
            else if (parameter instanceof I64 value) generator.writeStringField("value", value.value());
            else if (parameter instanceof U64 value) generator.writeStringField("value", value.value());
            else if (parameter instanceof Decimal value) generator.writeStringField("value", value.value());
            else if (parameter instanceof F64 value) generator.writeNumberField("value", value.value());
            else if (parameter instanceof Bool value) generator.writeBooleanField("value", value.value());
            else if (parameter instanceof Binary value) generator.writeStringField("value", value.value());
            generator.writeEndObject();
        }
    }

    /** Rejects unknown fields, variants and scalar coercions. */
    final class Deserializer extends JsonDeserializer<Parameter> {
        @Override public Parameter deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            JsonNode node = context.readTree(parser);
            try {
                if (!node.isObject()) throw new IllegalArgumentException("Parameter must be an object");
                var fields = node.fieldNames();
                while (fields.hasNext()) if (!Set.of("type", "value").contains(fields.next())) throw new IllegalArgumentException("Unknown parameter field");
                JsonNode type = node.get("type");
                if (type == null || !type.isTextual()) throw new IllegalArgumentException("Parameter type must be a string");
                JsonNode value = node.get("value");
                if (type.textValue().equals("null")) {
                    if (value != null) throw new IllegalArgumentException("null parameter must omit value");
                    return nullValue();
                }
                if (value == null || value.isNull()) throw new IllegalArgumentException("Parameter value is required");
                return switch (type.textValue()) {
                    case "string" -> string(text(value));
                    case "i64" -> i64(text(value));
                    case "u64" -> u64(text(value));
                    case "decimal" -> decimal(text(value));
                    case "binary" -> binary(text(value));
                    case "f64" -> {
                        if (!value.isNumber()) throw new IllegalArgumentException("f64 value must be a number");
                        yield f64(value.doubleValue());
                    }
                    case "bool" -> {
                        if (!value.isBoolean()) throw new IllegalArgumentException("bool value must be a boolean");
                        yield bool(value.booleanValue());
                    }
                    default -> throw new IllegalArgumentException("Unknown parameter type: " + type.textValue());
                };
            } catch (IllegalArgumentException exception) {
                throw JsonMappingException.from(parser, exception.getMessage(), exception);
            }
        }
        private static String text(JsonNode value) {
            if (!value.isTextual()) throw new IllegalArgumentException("Parameter value must be a string");
            return value.textValue();
        }
    }
}
