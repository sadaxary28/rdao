package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class ReflectionDatabaseException extends DatabaseException {

    public ReflectionDatabaseException(String message) {
        super(message);
    }

    public ReflectionDatabaseException(Throwable cause) {
        super(cause);
    }
}
