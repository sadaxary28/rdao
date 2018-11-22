package com.infomaximum.database.schema;

import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.utils.IntervalIndexUtils;

import java.util.Collection;
import java.util.List;

public class RangeIndex extends BaseIntervalIndex {

    private final static String INDEX_NAME = "rng";
    private final static byte[] INDEX_NAME_BYTES = INDEX_NAME.getBytes();

    private final List<Field> hashedFields;
    private final Field beginIndexedField;
    private final Field endIndexedField;

    public RangeIndex(com.infomaximum.database.anotation.RangeIndex index, StructEntity parent) {
        super(buildIndexedFields(index, parent), parent);

        this.hashedFields = sortedFields.subList(0, sortedFields.size() - 2);
        this.beginIndexedField = sortedFields.get(sortedFields.size() - 2);
        this.endIndexedField = sortedFields.get(sortedFields.size() - 1);
    }

    @Override
    public List<Field> getHashedFields() {
        return hashedFields;
    }

    public Field getBeginIndexedField() {
        return beginIndexedField;
    }

    public Field getEndIndexedField() {
        return endIndexedField;
    }

    @Override
    public void checkIndexedValueType(Class<?> valueType) {
        beginIndexedField.throwIfNotMatch(valueType);
    }

    private static List<Field> buildIndexedFields(com.infomaximum.database.anotation.RangeIndex index, StructEntity parent) {
        Field beginField = parent.getField(index.beginField());
        IntervalIndexUtils.checkType(beginField.getType());

        Field endField = parent.getField(index.endField());
        IntervalIndexUtils.checkType(endField.getType());

        if (beginField.getType() != endField.getType()) {
            throw new IllegalTypeException("Inconsistent range-types, " + beginField.getType().getSimpleName() + " and " + endField.getType().getSimpleName());
        }

        List<Field> fields = buildIndexedFields(index.hashedFields(), parent);
        fields.add(beginField);
        fields.add(endField);
        return fields;
    }

    public static String toString(Collection<String> hashedFields, String beginField, String endField) {
        return RangeIndex.class.getSimpleName() + ": " + hashedFields + ", [" + beginField + " - " + endField + "]";
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
