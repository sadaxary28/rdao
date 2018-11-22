package com.infomaximum.database.schema;

import java.util.Collection;

public class PrefixIndex extends BaseIndex {

    public final static String INDEX_NAME = "prf";
    public final static byte[] INDEX_NAME_BYTES = INDEX_NAME.getBytes();

    PrefixIndex(com.infomaximum.database.anotation.PrefixIndex index, StructEntity parent) {
        super(buildIndexedFields(index.fields(), parent), parent);
    }

    public static String toString(Collection<String> indexedFields) {
        return PrefixIndex.class.getSimpleName() + ": " + indexedFields;
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
