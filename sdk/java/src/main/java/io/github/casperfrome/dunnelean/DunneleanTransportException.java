package io.github.casperfrome.dunnelean;

/** Network or HTTP transport failure; remote submission or commit status may be uncertain. */
public final class DunneleanTransportException extends DunneleanException {
    private static final long serialVersionUID = 1L;

    public DunneleanTransportException(Throwable cause) {
        this("Dunnelean HTTP transport failed", cause);
    }

    public DunneleanTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
