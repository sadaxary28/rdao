package com.infomaximum.database.domainobject.filter;

public class PrefixIndexFilter implements Filter {

    private final String fieldName;
    private String fieldValue;

    public PrefixIndexFilter(String fieldName, String fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldValue() {
        return fieldValue;
    }

    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }
}
