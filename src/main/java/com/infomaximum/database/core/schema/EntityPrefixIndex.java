package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;

public class EntityPrefixIndex extends BaseIndex {

    EntityPrefixIndex(Index index, StructEntity parent) {
        super(index, parent);
    }

    @Override
    String getTypeMarker() {
        return "prefixindex";
    }
}
