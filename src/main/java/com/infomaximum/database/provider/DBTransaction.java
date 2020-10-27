package com.infomaximum.database.provider;

import com.infomaximum.database.exception.DatabaseException;

public interface DBTransaction extends AutoCloseable, DBDataCommand {

    void singleDeleteRange(String columnFamily, KeyPattern keyPattern) throws DatabaseException;

    void commit() throws DatabaseException;
    void rollback() throws DatabaseException;

    void compactRange() throws DatabaseException;

    @Override
    void close() throws DatabaseException;
}
