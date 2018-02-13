package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;

import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseIndex {

    public final String columnFamily;
    public final List<EntityField> sortedFields;

    BaseIndex(Index index, StructEntity parent) {
        this(buildIndexedFields(index.fields(), parent), parent);
    }

    BaseIndex(EntityField field, StructEntity parent) {
        this(Collections.singletonList(field), parent);
    }

    BaseIndex(List<EntityField> sortedIndexedFields, StructEntity parent) {
        this.sortedFields = Collections.unmodifiableList(sortedIndexedFields);
        this.columnFamily = buildColumnFamilyName(getIndexName(), parent.getColumnFamily(), this.sortedFields);
    }

    protected abstract String getIndexName();

    protected static List<EntityField> buildIndexedFields(String[] indexedFields, StructEntity parent) {
        return Arrays.stream(indexedFields)
                .sorted(Comparator.comparing(String::toLowerCase)) //Сортируем, что бы хеш не ломался из-за перестановки местами полей
                .map(parent::getField)
                .collect(Collectors.toList());
    }

    private static String buildColumnFamilyName(String indexName, String parentColumnFamily, List<EntityField> indexedFields) {
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(StructEntity.NAMESPACE_SEPARATOR).append(indexName);
        indexedFields.forEach(field -> stringBuilder.append('.').append(field.getName()).append(':').append(field.getType().getName()));
        return stringBuilder.toString();
    }
}