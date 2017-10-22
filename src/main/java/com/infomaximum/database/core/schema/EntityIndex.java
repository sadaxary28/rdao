package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;

public class EntityIndex extends BaseIndex {

    EntityIndex(Index index, StructEntity parent) {
        super(index, parent);
    }

    EntityIndex(EntityField field, StructEntity parent) {
        super(field, parent);
    }

    @Override
    String getTypeMarker() {
        return "index";
    }
}
