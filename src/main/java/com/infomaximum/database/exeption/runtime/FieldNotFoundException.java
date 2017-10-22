package com.infomaximum.database.exeption.runtime;

public class FieldNotFoundException extends RuntimeException {

    public FieldNotFoundException(Class<?> clazz, String fieldName) {
        super("Field " + fieldName + " not found in " + clazz);
    }
}
