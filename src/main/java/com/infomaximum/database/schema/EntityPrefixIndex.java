package com.infomaximum.database.schema;

import com.infomaximum.database.anotation.Index;

public class EntityPrefixIndex extends BaseIndex {

    EntityPrefixIndex(Index index, StructEntity parent) {
        super(index, parent);
    }

    @Override
    protected String getIndexName() {
        return "prefixindex";
    }
}
