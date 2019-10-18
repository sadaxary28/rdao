package com.infomaximum.database.exception;

public class TableNotFoundException extends SchemaException {

    public TableNotFoundException(String tableName) {
        super("Table name=" + tableName + " not found");
    }

    public TableNotFoundException(int tableId) {
        super("Table id=" + tableId + " not found");
    }
}
