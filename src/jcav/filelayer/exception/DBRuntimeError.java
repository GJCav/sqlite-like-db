package jcav.filelayer.exception;

/**
 * the program encountered some unrecoverable error at runtime.
 * the database file is possibly corrupted, or there are bugs in code.
 */
public class DBRuntimeError extends RuntimeException {
    public DBRuntimeError() {
    }

    public DBRuntimeError(String message) {
        super(message);
    }

    public DBRuntimeError(String message, Throwable cause) {
        super(message, cause);
    }

    public DBRuntimeError(Throwable cause) {
        super(cause);
    }

    public DBRuntimeError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
