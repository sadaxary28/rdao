package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class IteratorDataSourceDatabaseException extends DataSourceDatabaseException {

    public IteratorDataSourceDatabaseException(String message) {
        super(message);
    }

    public IteratorDataSourceDatabaseException(Throwable cause) {
        super(cause);
    }
}
