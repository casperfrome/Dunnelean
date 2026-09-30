package io.github.casperfrome.dunnelean;

/** Non-success HTTP response, with the service's error payload when available. */
public final class DunneleanApiException extends DunneleanException {
    private static final long serialVersionUID = 1L;
    private final int statusCode;
    private final ApiError error;
    private final String responseBody;

    public DunneleanApiException(int statusCode, ApiError error, String responseBody) {
        super(error == null ? "Dunnelean returned HTTP " + statusCode
                : "Dunnelean returned HTTP " + statusCode + ": " + error.code() + ": " + error.message());
        this.statusCode = statusCode;
        this.error = error;
        this.responseBody = responseBody;
    }

    public int statusCode() {
        return statusCode;
    }

    /** Structured error, or null when the response had no valid service error payload. */
    public ApiError error() {
        return error;
    }

    /** Response excerpt bounded by the client. */
    public String responseBody() {
        return responseBody;
    }

    /** Service error code, or null if unavailable. */
    public String code() {
        return error == null ? null : error.code();
    }

    /** Service message, or the exception message when no structured error was available. */
    public String message() {
        return error == null ? getMessage() : error.message();
    }

    /** Service flag; inspect {@link #error()} to distinguish absent structured information. */
    public boolean commitUnknown() {
        return error != null && error.commitUnknown();
    }

    /** Service retry suggestion; SDK calls never automatically retry submissions. */
    public boolean retryable() {
        return error != null && error.retryable();
    }
}
