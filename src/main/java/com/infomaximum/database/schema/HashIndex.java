package com.infomaximum.database.schema;

import java.util.Collection;
import java.util.Collections;

public class HashIndex extends BaseIndex {

    HashIndex(com.infomaximum.database.anotation.HashIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    HashIndex(Field field, StructEntity parent) {
        super(Collections.singletonList(field), parent);
    }

    @Override
    protected String getIndexName() {
        return "index";
    }

    public static String toString(Collection<String> indexedFields) {
        return HashIndex.class.getSimpleName() + ": " + indexedFields;
    }
}
