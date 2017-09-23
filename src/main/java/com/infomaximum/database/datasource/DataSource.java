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

    EntitySource getEntitySource(String columnFamily, long id, Set<String> fields) throws DataSourceDatabaseException;

    EntitySource findNextEntitySource(String columnFamily, Long prevId, String indexColumnFamily, int hash, Set<String> fields) throws DataSourceDatabaseException;

    EntitySource nextEntitySource(long iteratorId, Long prevId, Set<String> fields) throws DataSourceDatabaseException;

    void commit(List<Modifier> modifiers) throws DataSourceDatabaseException;

    long createIterator(String columnFamily) throws DataSourceDatabaseException;

    void closeIterator(long iteratorId);

    void createColumnFamily(String name) throws DataSourceDatabaseException;
    void dropColumnFamily(String name) throws DataSourceDatabaseException;
    void createSequence(String name) throws DataSourceDatabaseException;
    void dropSequence(String name) throws DataSourceDatabaseException;
}
