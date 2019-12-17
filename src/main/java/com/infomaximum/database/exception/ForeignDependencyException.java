package com.infomaximum.database.exception;

import com.infomaximum.database.schema.newschema.Field;

public class ForeignDependencyException extends DatabaseException {

    public ForeignDependencyException(long objId, Class objClass, Field foreignField, long notExistenceFieldValue) {
        super(String.format("Foreign field %s.%s = %d not exists into %s, for object id = %d.",
                objClass.getName(), foreignField.getName(), notExistenceFieldValue,
                foreignField.getForeignDependency().getObjectClass().getName(), objId));
    }

    public ForeignDependencyException(long removingId, Class removingClass, long referencingId, Class referencingClass) {
        super(String.format("Object %s.id = %d referenced to removing %s.id = %d.",
                referencingClass.getName(), referencingId,
                removingClass.getName(), removingId));
    }
}
