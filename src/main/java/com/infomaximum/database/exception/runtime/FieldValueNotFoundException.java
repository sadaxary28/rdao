package com.infomaximum.database.exception.runtime;

public class FieldValueNotFoundException extends RuntimeException {

    public FieldValueNotFoundException(String fieldName) {
        super(fieldName);
    }
}
