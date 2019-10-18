package com.infomaximum.database.exception;

import com.infomaximum.database.domainobject.DomainObject;

public class FieldAlreadyExistsException extends SchemaException {

    public FieldAlreadyExistsException(String fieldName, String tableName) {
        super("Field name=" + fieldName + " already exists into '" + tableName + "'");
    }

    public FieldAlreadyExistsException(int fieldNumber, Class<? extends DomainObject> objClass) {
        super("Field number=" + fieldNumber + " already exists into " + objClass.getSimpleName());
    }
}
