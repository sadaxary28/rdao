package com.infomaximum.database.exception.runtime;

public class DatabaseRuntimeException extends RuntimeException {

    public DatabaseRuntimeException(String message) {
        super(message);
    }

    public DatabaseRuntimeException(Throwable cause) {
        super(cause);
    }

    public DatabaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
