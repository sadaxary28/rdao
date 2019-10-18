package com.infomaximum.database.exception;

import com.infomaximum.database.schema.BaseIndex;

public class IndexAlreadyExistsException extends SchemaException {

    public <T extends BaseIndex> IndexAlreadyExistsException(T index) {
        super("Index already exists, " + index.toString());
    }
}
