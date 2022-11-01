package com.infomaximum.database.exception;

public class ForeignDependencyAlreadyExistException extends SchemaException {

    public ForeignDependencyAlreadyExistException(String fieldName, String tableName) {
        super("Field name=" + fieldName + " into '" + tableName + "' is already foreign key");
    }
}