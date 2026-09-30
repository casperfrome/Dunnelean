package io.github.casperfrome.dunnelean;

/** A success response that does not match the expected service protocol. */
public final class DunneleanProtocolException extends DunneleanException {
    private static final long serialVersionUID = 1L;
    private final int statusCode;
    private final String responseBody;

    public DunneleanProtocolException(int statusCode, String responseBody, Throwable cause) {
        this("Invalid Dunnelean response for HTTP " + statusCode, statusCode, responseBody, cause);
    }

    public DunneleanProtocolException(String message, int statusCode, String responseBody, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public int statusCode() {
        return statusCode;
    }

    /** Response excerpt bounded by the client. */
    public String responseBody() {
        return responseBody;
    }
}
