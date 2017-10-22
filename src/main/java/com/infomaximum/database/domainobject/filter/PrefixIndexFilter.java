package com.infomaximum.database.domainobject.filter;

import java.util.*;

public class PrefixIndexFilter implements Filter {

    private final Set<String> fieldNames;
    private String fieldValue;

    public PrefixIndexFilter(String fieldName, String fieldValue) {
        this.fieldNames = Collections.singleton(fieldName);
        this.fieldValue = fieldValue;
    }

    public PrefixIndexFilter(List<String> fieldNames, String fieldValue) {
        this.fieldNames = new HashSet<>(fieldNames);
        this.fieldValue = fieldValue;
    }

    public Collection<String> getFieldNames() {
        return fieldNames;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }
}
