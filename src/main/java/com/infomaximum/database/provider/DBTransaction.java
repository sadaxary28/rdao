package com.infomaximum.database.provider;

import com.infomaximum.database.exception.DatabaseException;

public interface DBTransaction extends AutoCloseable {

    DBIterator createIterator(String columnFamily) throws DatabaseException;
    long nextId(String sequenceName) throws DatabaseException;
    byte[] getValue(String columnFamily, byte[] key) throws DatabaseException;

    void put(String columnFamily, byte[] key, byte[] value) throws DatabaseException;

    void delete(String columnFamily, byte[] key) throws DatabaseException;
    /**
     * @param beginKey inclusive
     * @param endKey exclusive
     */
    void deleteRange(String columnFamily, byte[] beginKey, byte[] endKey) throws DatabaseException;

    void singleDelete(String columnFamily, byte[] key) throws DatabaseException;
    /**
     * @param beginKey inclusive
     * @param endKey exclusive
     */
    void singleDeleteRange(String columnFamily, byte[] beginKey, byte[] endKey) throws DatabaseException;

    void singleDeleteRange(String columnFamily, KeyPattern keyPattern) throws DatabaseException;

    void commit() throws DatabaseException;
    void rollback() throws DatabaseException;

    @Override
    void close() throws DatabaseException;
}
