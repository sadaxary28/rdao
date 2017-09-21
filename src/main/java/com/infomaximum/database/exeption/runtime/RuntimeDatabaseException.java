package com.infomaximum.database.exeption.runtime;

/**
 * Created by kris on 08.09.17.
 */
public class RuntimeDatabaseException extends RuntimeException {

    public RuntimeDatabaseException(String message) {
        super(message);
    }

    public RuntimeDatabaseException(Throwable cause) {
        super(cause);
    }
}
