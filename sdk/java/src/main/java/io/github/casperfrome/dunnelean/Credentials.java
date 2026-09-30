package io.github.casperfrome.dunnelean;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

/** Credentials with exactly one explicit password or server-side environment variable. */
@JsonDeserialize(builder = Credentials.Builder.class)
public record Credentials(String username, String password, String passwordEnv) {
    public Credentials {
        ConfigValues.required(username, "username");
        if ((password == null) == (passwordEnv == null)) throw new IllegalArgumentException("Specify exactly one of password or passwordEnv; an empty password is explicit");
    }
    public static Builder builder() { return new Builder(); }
    public Builder toBuilder() { return new Builder().username(username).password(password).passwordEnv(passwordEnv); }
    /** Hides the explicit password, including when this value is printed within another record. */
    @Override public String toString() {
        return "Credentials[username=" + username + ", password=" + (password == null ? "null" : "[REDACTED]") + ", passwordEnv=" + passwordEnv + "]";
    }
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.NONE)
    @JsonPOJOBuilder(withPrefix = "")
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static final class Builder {
        private String username;
        private String password;
        private String passwordEnv;
        public Builder() {}
        public Builder username(String value) { username = value; return this; }
        public Builder password(String value) { password = value; return this; }
        public Builder passwordEnv(String value) { passwordEnv = value; return this; }
        public Credentials build() { return new Credentials(username, password, passwordEnv); }
    }
}
