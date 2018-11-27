package com.infomaximum.database.schema;

import com.infomaximum.database.utils.IntervalIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.util.Collection;
import java.util.List;

public class IntervalIndex extends BaseIntervalIndex {

    private final static String INDEX_NAME = "int";
    public final static byte[] INDEX_NAME_BYTES = TypeConvert.pack(INDEX_NAME);

    private final List<Field> hashedFields;
    private final Field indexedField;

    public IntervalIndex(com.infomaximum.database.anotation.IntervalIndex index, StructEntity parent) {
        super(buildIndexedFields(index, parent), parent);

        this.hashedFields = sortedFields.subList(0, sortedFields.size() - 1);
        this.indexedField = sortedFields.get(sortedFields.size() - 1);
    }

    @Override
    public List<Field> getHashedFields() {
        return hashedFields;
    }

    public Field getIndexedField() {
        return indexedField;
    }

    @Override
    public void checkIndexedValueType(Class<?> valueType) {
        indexedField.throwIfNotMatch(valueType);
    }

    private static List<Field> buildIndexedFields(com.infomaximum.database.anotation.IntervalIndex index, StructEntity parent) {
        Field indexedField = parent.getField(index.indexedField());
        IntervalIndexUtils.checkType(indexedField.getType());

        List<Field> fields = buildIndexedFields(index.hashedFields(), parent);
        fields.add(indexedField);
        return fields;
    }

    public static String toString(Collection<String> hashedFields, String indexedField) {
        return IntervalIndex.class.getSimpleName() + ": " + hashedFields + ", " + indexedField;
    }

    @Override
    public byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }
}
