package com.infomaximum.database.exeption;

import com.infomaximum.database.core.schema.EntityField;

public class ForeignDependencyException extends DatabaseException {

    public ForeignDependencyException(long objId, Class objClass, EntityField foreignField, long notExistenceFieldValue) {
        super(String.format("Field %s.%s = %d referenced to nonexistent object %s.id = %d.",
                objClass, foreignField.getName(), objId,
                foreignField.getForeignDependency().getObjectClass(), notExistenceFieldValue));
    }

    public ForeignDependencyException(long removingId, Class removingClass, long referencingId, Class referencingClass) {
        super(String.format("Removing object %s.id = %d referenced to %s.id = %d.",
                removingClass, removingId,
                referencingClass, referencingId));
    }
}
