package com.infomaximum.database.domainobject.filter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PrefixFilter implements Filter {

    private final Set<String> fieldNames;
    private String fieldValue;

    public PrefixFilter(String fieldName, String fieldValue) {
        this.fieldNames = Collections.singleton(fieldName);
        this.fieldValue = fieldValue;
    }

    public PrefixFilter(Collection<String> fieldNames, String fieldValue) {
        this.fieldNames = Collections.unmodifiableSet(new HashSet<>(fieldNames));
        this.fieldValue = fieldValue;
    }

    public Set<String> getFieldNames() {
        return fieldNames;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }
}
