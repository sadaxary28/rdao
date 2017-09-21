package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class DataSourceDatabaseException extends DatabaseException {

    public DataSourceDatabaseException(String message) {
        super(message);
    }

    public DataSourceDatabaseException(Throwable cause) {
        super(cause);
    }
}
