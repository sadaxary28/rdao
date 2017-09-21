package com.infomaximum.database.exeption.runtime;

/**
 * Created by kris on 08.09.17.
 */
public class ReflectionDatabaseException extends RuntimeDatabaseException {

    public ReflectionDatabaseException(String message) {
        super(message);
    }

    public ReflectionDatabaseException(Throwable cause) {
        super(cause);
    }
}
