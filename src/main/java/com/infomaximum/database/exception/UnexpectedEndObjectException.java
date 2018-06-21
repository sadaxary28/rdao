package com.infomaximum.database.exception;

import com.infomaximum.database.utils.key.FieldKey;

public class UnexpectedEndObjectException extends DatabaseException {

    public UnexpectedEndObjectException(long prevId, long nextId, String fieldName) {
        super("Unexpected end of object. Previous id of object: " + prevId + ". Next key: id = " + nextId + ", field = " + fieldName);
    }
}
