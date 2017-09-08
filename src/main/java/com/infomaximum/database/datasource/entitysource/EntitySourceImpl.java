package com.infomaximum.database.datasource.entitysource;

import java.util.Map;

/**
 * Created by kris on 01.05.17.
 */
public class EntitySourceImpl implements EntitySource {

    private final long id;
    private final Map<String, byte[]> fields;

    public EntitySourceImpl(long id, Map<String, byte[]> fields) {
        this.id = id;
        this.fields = fields;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Map<String, byte[]> getFields() {
        return fields;
    }
}
