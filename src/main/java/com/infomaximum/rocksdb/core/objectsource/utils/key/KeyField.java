package com.infomaximum.rocksdb.core.objectsource.utils.key;

/**
 * Created by kris on 27.04.17.
 */
public class KeyField extends Key {

    private final String fieldName;

    public KeyField(long id, String fieldName) {
        super(id);
        this.fieldName=fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public TypeKey getTypeKey() {
        return TypeKey.FIELD;
    }

    @Override
    public String pack() {
        return new StringBuilder()
                .append(id).append('.')
                .append(getTypeKey().getId()).append('.')
                .append(fieldName)
                .toString();
    }
}
