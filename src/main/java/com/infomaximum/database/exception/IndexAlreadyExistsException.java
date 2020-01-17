package com.infomaximum.database.exception;

import com.infomaximum.database.schema.BaseIndex;
import com.infomaximum.database.schema.dbstruct.DBIndex;

public class IndexAlreadyExistsException extends SchemaException {

    public <T extends BaseIndex> IndexAlreadyExistsException(T index) {
        super("Index already exists, " + index.toString());
    }

    public <T extends DBIndex> IndexAlreadyExistsException(T index) {
        super("Index already exists, " + index.toString());
    }
}
