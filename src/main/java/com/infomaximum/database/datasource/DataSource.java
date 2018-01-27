package com.infomaximum.database.datasource;

import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.exception.DataSourceDatabaseException;

import java.util.List;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    enum StepDirection {
        FORWARD, BACKWARD
    }

    long nextId(String sequenceName) throws DataSourceDatabaseException;

    byte[] getValue(String columnFamily, final byte[] key) throws DataSourceDatabaseException;
    byte[] getValue(String columnFamily, final byte[] key, long transactionId) throws DataSourceDatabaseException;

    long beginTransaction() throws DataSourceDatabaseException;
    void modify(final List<Modifier> modifiers, long transactionId) throws DataSourceDatabaseException;
    void commitTransaction(long transactionId) throws DataSourceDatabaseException;
    void rollbackTransaction(long transactionId) throws DataSourceDatabaseException;

    long createIterator(String columnFamily) throws DataSourceDatabaseException;
    long createIterator(String columnFamily, long transactionId) throws DataSourceDatabaseException;
    KeyValue seek(long iteratorId, final KeyPattern pattern) throws DataSourceDatabaseException;
    KeyValue next(long iteratorId) throws DataSourceDatabaseException;
    KeyValue step(long iteratorId, StepDirection direction) throws DataSourceDatabaseException;
    void closeIterator(long iteratorId);

    boolean containsColumnFamily(String name);
    String[] getColumnFamilies();
    void createColumnFamily(String name) throws DataSourceDatabaseException;
    void dropColumnFamily(String name) throws DataSourceDatabaseException;

    boolean containsSequence(String name);
    void createSequence(String name) throws DataSourceDatabaseException;
    void dropSequence(String name) throws DataSourceDatabaseException;
}
