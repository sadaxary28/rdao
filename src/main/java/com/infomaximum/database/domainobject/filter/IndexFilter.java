package com.infomaximum.database.domainobject.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndexFilter implements Filter {

    private final Map<String, Object> values = new HashMap<>();

    public IndexFilter(String fieldName, Object fieldValue) {
        appendField(fieldName, fieldValue);
    }

    public IndexFilter appendField(String name, Object value) {
        values.put(name, value);
        return this;
    }

    public IndexFilter removeField(String name) {
        values.remove(name);
        return this;
    }

    public Map<String, Object> getValues() {
        return Collections.unmodifiableMap(values);
    }
}
