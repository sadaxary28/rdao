package com.infomaximum.database.exeption.runtime;

public class IllegalTypeDatabaseException extends RuntimeDatabaseException {

    public IllegalTypeDatabaseException(String message) {
        super(message);
    }

    public IllegalTypeDatabaseException(Throwable cause) {
        super(cause);
    }

    public IllegalTypeDatabaseException(Class expected, Class actual) {
        super("Expected type " + expected + " but actual type " + actual);
    }
}
