package com.infomaximum.database.exeption;

import com.infomaximum.database.domainobject.key.FieldKey;

public class UnexpectedEndObjectException extends DataSourceDatabaseException {

    public UnexpectedEndObjectException(long prevId, FieldKey nextKey) {
        super("Unexpected end of object. Previous id of object: " + prevId + ". Next key: id = " + nextKey.getId() + ", field = " + nextKey.getFieldName());
    }
}
