package com.infomaximum.database.exeption.index;

import com.infomaximum.database.exeption.DatabaseException;

/**
 * Created by kris on 06.09.17.
 */
public class IndexDatabaseException extends DatabaseException {

    public IndexDatabaseException(String message) {
        super(message);
    }

    public IndexDatabaseException(Throwable cause) {
        super(cause);
    }
}

