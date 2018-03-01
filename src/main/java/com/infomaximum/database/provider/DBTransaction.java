package com.infomaximum.database.provider;

import com.infomaximum.database.exception.DatabaseException;

public interface DBTransaction extends AutoCloseable {

    DBIterator createIterator(String columnFamily) throws DatabaseException;
    long nextId(String sequenceName) throws DatabaseException;
    byte[] getValue(String columnFamily, byte[] key) throws DatabaseException;

    void put(String columnFamily, byte[] key, byte[] value) throws DatabaseException;
    void delete(String columnFamily, byte[] key) throws DatabaseException;
    void deleteRange(String columnFamily, byte[] keyPrefix) throws DatabaseException;

    void commit() throws DatabaseException;
    void rollback() throws DatabaseException;

    @Override
    void close() throws DatabaseException;
}
