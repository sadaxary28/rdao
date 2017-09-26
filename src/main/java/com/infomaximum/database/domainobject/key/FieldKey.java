package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.exeption.runtime.KeyCorruptedException;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class FieldKey extends Key {

    private final String fieldName;

    public FieldKey(long id) {
        super(id);
        this.fieldName = null;
    }

    public FieldKey(long id, String fieldName) {
        super(id);
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isBeginningObject() {
        return fieldName == null;
    }

    @Override
    public byte[] pack() {
        final byte[] name = fieldName != null ? TypeConvert.pack(fieldName) : null;
        final ByteBuffer buffer = TypeConvert.allocateBuffer(8 + (name != null ? name.length : 0));

        buffer.putLong(getId());
        if (name != null) {
            buffer.put(name);
        }

        return buffer.array();
    }

    public static FieldKey unpack(final byte[] src) {
        if (src.length < 8) {
            throw new KeyCorruptedException(src);
        }

        ByteBuffer buffer = TypeConvert.wrapBuffer(src);
        long id = buffer.getLong();
        if (src.length == 8) {
            return new FieldKey(id);
        }

        return new FieldKey(id, TypeConvert.getString(src, 8, src.length - 8));
    }

    public static byte[] getKeyPrefix(long id) {
        return TypeConvert.pack(id);
    }
}
