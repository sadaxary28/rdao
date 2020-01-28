package com.infomaximum.database.exception;

import com.infomaximum.database.exception.runtime.SchemaException;

public class FieldNotFoundException extends SchemaException {

    public FieldNotFoundException(String fieldName, String tableName) {
        super("Field name=" + fieldName + " not found into '" + tableName + "'");
    }

    public FieldNotFoundException(int fieldId, String tableName) {
        super("Field id=" + fieldId + " not found into '" + tableName + "'");
    }
}
