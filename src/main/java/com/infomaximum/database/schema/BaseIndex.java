package com.infomaximum.database.schema;

import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseIndex {

    public final String columnFamily;
    public final List<Field> sortedFields;

    BaseIndex(List<Field> sortedIndexedFields, StructEntity parent) {
        this.sortedFields = Collections.unmodifiableList(sortedIndexedFields);
        this.columnFamily = buildColumnFamilyName(getIndexName(), parent.getColumnFamily(), this.sortedFields);
    }

    protected abstract String getIndexName();

    protected static List<Field> buildIndexedFields(int[] indexedFields, StructEntity parent) {
        return Arrays.stream(indexedFields)
                .mapToObj(parent::getField)
                .sorted(Comparator.comparing(f -> f.getName().toLowerCase())) //Сортируем, что бы хеш не ломался из-за перестановки местами полей
                .collect(Collectors.toList());
    }

    private static String buildColumnFamilyName(String indexName, String parentColumnFamily, List<Field> indexedFields) {
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(StructEntity.NAMESPACE_SEPARATOR).append(indexName);
        indexedFields.forEach(field -> stringBuilder.append('.').append(field.getName()).append(':').append(field.getType().getName()));
        return stringBuilder.toString();
    }
}