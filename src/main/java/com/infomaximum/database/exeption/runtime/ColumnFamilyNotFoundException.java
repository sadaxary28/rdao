package com.infomaximum.database.exeption.runtime;

public class ColumnFamilyNotFoundException extends RuntimeException {

    public ColumnFamilyNotFoundException(String columnFamily) {
        super("Column family " + columnFamily + " not found.");
    }
}
