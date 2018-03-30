package com.infomaximum.database.exception;

import com.infomaximum.database.utils.key.FieldKey;

public class UnexpectedEndObjectException extends DatabaseException {

    public UnexpectedEndObjectException(long prevId, FieldKey nextKey) {
        super("Unexpected end of object. Previous id of object: " + prevId + ". Next key: id = " + nextKey.getId() + ", field = " + nextKey.getFieldName());
    }
}
