package com.infomaximum.database.domainobject;

import java.util.Map;

public class EntitySource {

    private final long id;
    private final Map<String, byte[]> fields;

    public EntitySource(long id, Map<String, byte[]> fields) {
        this.id = id;
        this.fields = fields;
    }

    public long getId() {
        return id;
    }

    public Map<String, byte[]> getFields() {
        return fields;
    }
}
