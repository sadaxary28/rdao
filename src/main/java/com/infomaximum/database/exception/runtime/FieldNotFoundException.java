package com.infomaximum.database.exception.runtime;

public class FieldNotFoundException extends RuntimeException {

    public FieldNotFoundException(Class<?> clazz, String fieldName) {
        super("Field " + fieldName + " not found in " + clazz);
    }
}
