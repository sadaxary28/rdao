package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class KeyDatabaseException extends DatabaseException {

    public KeyDatabaseException(String message) {
        super(message);
    }

    public KeyDatabaseException(Throwable cause) {
        super(cause);
    }

    public KeyDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
