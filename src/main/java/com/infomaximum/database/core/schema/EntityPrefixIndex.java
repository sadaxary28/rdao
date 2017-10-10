package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.PrefixIndex;

public class EntityPrefixIndex {

    public final String columnFamily;
    public final EntityField field;

    protected EntityPrefixIndex(PrefixIndex index, StructEntity parent) {
        this.field = parent.getField(index.name());
        this.columnFamily = buildColumnFamilyName(parent.getColumnFamily(), index.name());

        this.field.throwIfNotMatch(String.class);
    }

    private static String buildColumnFamilyName(String parentColumnFamily, String fieldName){
        return new StringBuilder(parentColumnFamily).append(StructEntity.NAMESPACE_SEPARATOR).append("prefixtextindex.").append(fieldName).toString();
    }
}
