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

    public long nextId(String sequenceName) throws DataSourceDatabaseException;

    public byte[] getField(String columnFamily, long id, String field) throws DataSourceDatabaseException;

    public EntitySource getEntitySource(String columnFamily, long id, Set<String> fields) throws DataSourceDatabaseException;

    public EntitySource findNextEntitySource(String columnFamily, Long prevId, String indexColumnFamily, int hash, Set<String> fields) throws DataSourceDatabaseException;

    public EntitySource nextEntitySource(String columnFamily, Long prevId, Set<String> fields) throws DataSourceDatabaseException;

    public void commit(List<Modifier> modifiers) throws DataSourceDatabaseException;

    void createColumnFamily(String name) throws DataSourceDatabaseException;
    void dropColumnFamily(String name) throws DataSourceDatabaseException;
    void createSequence(String name) throws DataSourceDatabaseException;
    void dropSequence(String name) throws DataSourceDatabaseException;
}
