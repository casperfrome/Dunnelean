package io.github.casperfrome.dunnelean;

/** Base exception for SDK failures; synchronous interruption remains an InterruptedException. */
public class DunneleanException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public DunneleanException(String message) {
        super(message);
    }

    public DunneleanException(String message, Throwable cause) {
        super(message, cause);
    }
}
