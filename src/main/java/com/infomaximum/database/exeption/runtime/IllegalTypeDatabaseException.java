package com.infomaximum.database.exeption.runtime;

public class IllegalTypeDatabaseException extends RuntimeDatabaseException {

    public IllegalTypeDatabaseException(String message) {
        super(message);
    }

    public IllegalTypeDatabaseException(Throwable cause) {
        super(cause);
    }
}
