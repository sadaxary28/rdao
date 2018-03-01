package com.infomaximum.database.schema;

import com.infomaximum.database.anotation.Index;

public class EntityIndex extends BaseIndex {

    EntityIndex(Index index, StructEntity parent) {
        super(index, parent);
    }

    EntityIndex(EntityField field, StructEntity parent) {
        super(field, parent);
    }

    @Override
    protected String getIndexName() {
        return "index";
    }
}
