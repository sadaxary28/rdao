package com.infomaximum.database.schema;

import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.KeyUtils;

import java.util.*;
import java.util.stream.Collectors;

public abstract class BaseIndex {

    public static final int FIELDS_HASH_BYTE_SIZE = 4;
    public static final int INDEX_NAME_BYTE_SIZE = 3;
    public static final int ATTENDANT_BYTE_SIZE = INDEX_NAME_BYTE_SIZE + FIELDS_HASH_BYTE_SIZE;

    public final byte[] attendant;
    public final String columnFamily;
    public final List<Field> sortedFields;

    BaseIndex(List<Field> sortedIndexedFields, StructEntity parent) {
        this.sortedFields = Collections.unmodifiableList(sortedIndexedFields);
        this.columnFamily = parent.getIndexColumnFamily();
        this.attendant = new byte[ATTENDANT_BYTE_SIZE];
        KeyUtils.putAttendantBytes(this.attendant, getIndexNameBytes(), buildFieldsHashCRC32(sortedIndexedFields));
    }

    static List<Field> buildIndexedFields(int[] indexedFields, StructEntity parent) {
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

    protected abstract byte[] getIndexNameBytes();
}