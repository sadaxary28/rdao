package com.infomaximum.database.schema;

import com.infomaximum.database.utils.TypeConvert;

import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseIndex {

    public final byte[] fieldsHash;
    public final String columnFamily;
    public final List<Field> sortedFields;

    BaseIndex(List<Field> sortedIndexedFields, StructEntity parent) {
        this.sortedFields = Collections.unmodifiableList(sortedIndexedFields);
        this.columnFamily = parent.getIndexColumnFamily();
        this.fieldsHash = buildFieldsHashCRC32(sortedIndexedFields);
    }

    protected static List<Field> buildIndexedFields(int[] indexedFields, StructEntity parent) {
        return Arrays.stream(indexedFields)
                .mapToObj(parent::getField)
                .sorted(Comparator.comparing(f -> f.getName().toLowerCase())) //Сортируем, что бы хеш не ломался из-за перестановки местами полей
                .collect(Collectors.toList());
    }

    private static byte[] buildFieldsHashCRC32(List<Field> indexedFields) {
        StringBuilder stringBuilder = new StringBuilder();
        indexedFields.forEach(field -> stringBuilder.append(field.getName()).append(':').append(field.getType().getName()).append(StructEntity.NAMESPACE_SEPARATOR));
        return TypeConvert.packCRC32(stringBuilder.toString());
    }

    public abstract String getIndexName();

    public abstract byte[] getIndexNameBytes();
}