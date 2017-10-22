package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class IteratorNotFoundException extends DataSourceDatabaseException {

    public IteratorNotFoundException(long iteratorId) {
        super(String.format("Iterator #%d not found.", iteratorId));
    }
}
