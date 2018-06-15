package com.infomaximum.database.domainobject.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class HashFilter implements Filter {

    private final Map<String, Object> values = new HashMap<>();

    public HashFilter(String fieldName, Object fieldValue) {
        appendField(fieldName, fieldValue);
    }

    public HashFilter appendField(String name, Object value) {
        values.put(name, value);
        return this;
    }

    public Map<String, Object> getValues() {
        return Collections.unmodifiableMap(values);
    }
}
