package com.infomaximum.database.exception.runtime;

public class SchemaException extends RuntimeException {

    public SchemaException(Throwable cause) {
        super(cause);
    }

    public SchemaException(String message) {
        super(message);
    }
}
