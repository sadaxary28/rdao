package com.infomaximum.database.schema;

import java.util.Collection;

public class PrefixIndex extends BaseIndex {

    PrefixIndex(com.infomaximum.database.anotation.PrefixIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    public static String toString(Collection<String> indexedFields) {
        return PrefixIndex.class.getSimpleName() + ": " + indexedFields;
    }
}
