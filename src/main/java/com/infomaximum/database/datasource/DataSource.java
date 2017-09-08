package com.infomaximum.database.datasource;

import com.infomaximum.database.core.transaction.struct.modifier.Modifier;
import com.infomaximum.database.datasource.entitysource.EntitySource;

import java.util.List;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    public long nextId(String columnFamily);

    public byte[] getField(String columnFamily, long id, String field);

    public EntitySource getEntitySource(String columnFamily, long id, Set<String> fields);

    public EntitySource findNextEntitySource(String columnFamily, Long prevId, String index, int hash, Set<String> fields);

    public EntitySource nextEntitySource(String columnFamily, Long prevId, Set<String> fields);

    public void commit(List<Modifier> modifiers);
}
