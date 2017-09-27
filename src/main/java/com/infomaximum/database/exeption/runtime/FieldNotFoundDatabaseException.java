package com.infomaximum.database.exeption.runtime;

import com.infomaximum.database.exeption.DatabaseException;

public class FieldNotFoundDatabaseException extends RuntimeDatabaseException {

    public FieldNotFoundDatabaseException(Class<?> clazz, String fieldName) {
        super("Field " + fieldName + " not found in " + clazz);
    }
}
