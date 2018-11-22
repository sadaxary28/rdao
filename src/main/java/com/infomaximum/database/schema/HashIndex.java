package com.infomaximum.database.schema;

import java.util.Collection;
import java.util.Collections;

public class HashIndex extends BaseIndex {

    public final static String INDEX_NAME = "hsh";
    public final static byte[] INDEX_NAME_BYTES = INDEX_NAME.getBytes();

    HashIndex(com.infomaximum.database.anotation.HashIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    HashIndex(Field field, StructEntity parent) {
        super(Collections.singletonList(field), parent);
    }

    public static String toString(Collection<String> indexedFields) {
        return HashIndex.class.getSimpleName() + ": " + indexedFields;
    }

    @Override
    public String getIndexName() {
        return INDEX_NAME;
    }

    @Override
    public byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }
}
