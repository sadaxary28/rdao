package com.infomaximum.database.datasource;

import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

import java.util.List;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    long nextId(String sequenceName) throws DataSourceDatabaseException;

    byte[] getValue(String columnFamily, final byte[] key) throws DataSourceDatabaseException;
    byte[] getValue(String columnFamily, final byte[] key, long transactionId) throws DataSourceDatabaseException;

    long beginTransaction() throws DataSourceDatabaseException;
    void modify(final List<Modifier> modifiers, long transactionId) throws DataSourceDatabaseException;
    void commitTransaction(long transactionId) throws DataSourceDatabaseException;
    void rollbackTransaction(long transactionId) throws DataSourceDatabaseException;

    long createIterator(String columnFamily, final KeyPattern pattern) throws DataSourceDatabaseException;
    long createIterator(String columnFamily, final KeyPattern pattern, long transactionId) throws DataSourceDatabaseException;
    void seekIterator(long iteratorId, final KeyPattern pattern) throws DataSourceDatabaseException;
    KeyValue next(long iteratorId) throws DataSourceDatabaseException;
    void closeIterator(long iteratorId);

    boolean containsColumnFamily(String name);
    String[] getColumnFamilies();
    void createColumnFamily(String name) throws DataSourceDatabaseException;
    void dropColumnFamily(String name) throws DataSourceDatabaseException;

    boolean containsSequence(String name);
    void createSequence(String name) throws DataSourceDatabaseException;
    void dropSequence(String name) throws DataSourceDatabaseException;
}
