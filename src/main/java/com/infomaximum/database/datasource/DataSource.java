package com.infomaximum.database.datasource;

import com.infomaximum.database.core.transaction.struct.modifier.Modifier;
import com.infomaximum.database.datasource.entitysource.EntitySource;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

import java.util.List;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    long nextId(String sequenceName) throws DataSourceDatabaseException;

    byte[] getField(String columnFamily, long id, String field) throws DataSourceDatabaseException;
    byte[] getField(String columnFamily, long id, String field, long transactionId) throws DataSourceDatabaseException;

    EntitySource getEntitySource(String columnFamily, long id, Set<String> fields) throws DataSourceDatabaseException;
    EntitySource getEntitySource(String columnFamily, long id, Set<String> fields, long transactionId) throws DataSourceDatabaseException;

    EntitySource findNextEntitySource(String columnFamily, Long prevId, String indexColumnFamily, int hash, Set<String> fields) throws DataSourceDatabaseException;

    EntitySource nextEntitySource(long iteratorId, Set<String> fields) throws DataSourceDatabaseException;

    void modify(final List<Modifier> modifiers, long transactionId) throws DataSourceDatabaseException;

    long beginTransaction() throws DataSourceDatabaseException;
    void commitTransaction(long transactionId) throws DataSourceDatabaseException;
    void rollbackTransaction(long transactionId) throws DataSourceDatabaseException;

    long createIterator(String columnFamily) throws DataSourceDatabaseException;
    long createIterator(String columnFamily, long transactionId) throws DataSourceDatabaseException;
    void closeIterator(long iteratorId);

    void createColumnFamily(String name) throws DataSourceDatabaseException;
    void dropColumnFamily(String name) throws DataSourceDatabaseException;
    void createSequence(String name) throws DataSourceDatabaseException;
    void dropSequence(String name) throws DataSourceDatabaseException;
}
