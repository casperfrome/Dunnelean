package io.github.casperfrome.dunnelean;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class ConfigValues {
    static final BigInteger UINT64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    static final BigInteger INT64_MIN = BigInteger.ONE.shiftLeft(63).negate();
    static final BigInteger INT64_MAX = BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE);
    private ConfigValues() {}
    static <T> T required(T value, String field) { return Objects.requireNonNull(value, field + " must not be null"); }
    static BigInteger uint64(BigInteger value, String field) {
        required(value, field);
        if (value.signum() < 0 || value.compareTo(UINT64_MAX) > 0) throw new IllegalArgumentException(field + " must be 0..18446744073709551615");
        return value;
    }
    static void range(int value, int lower, int upper, String field) {
        if (value < lower || value > upper) throw new IllegalArgumentException(field + " must be " + lower + ".." + upper);
    }
    static String bound(String value, String field) {
        BigInteger parsed = integer(value, field);
        if (parsed.compareTo(INT64_MIN) < 0 || parsed.compareTo(UINT64_MAX) > 0) throw new IllegalArgumentException(field + " exceeds the signed/unsigned 64-bit range");
        return value;
    }
    static BigInteger integer(String value, String field) {
        required(value, field);
        if (!value.matches("[+-]?[0-9]+")) throw new IllegalArgumentException(field + " must be an integer string");
        return new BigInteger(value);
    }
    static void requestId(String id) {
        if (id != null && (id.isEmpty() || id.getBytes(StandardCharsets.UTF_8).length > 200)) throw new IllegalArgumentException("requestId must be 1..200 UTF-8 bytes");
    }
}
