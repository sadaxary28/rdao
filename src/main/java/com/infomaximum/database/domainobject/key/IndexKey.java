package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.exeption.runtime.KeyCorruptedException;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class IndexKey extends Key {

    private final long[] fieldValues;

    public IndexKey(long id, final long[] fieldValues) {
        super(id);

        if (fieldValues == null || fieldValues.length == 0) {
            throw new IllegalArgumentException();
        }
        this.fieldValues = fieldValues;
    }

    public long[] getFieldValues() {
        return fieldValues;
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE + ID_BYTE_SIZE * fieldValues.length);
        for (int i = 0; i < fieldValues.length; ++i) {
            buffer.putLong(fieldValues[i]);
        }
        buffer.putLong(getId());
        return buffer.array();
    }

    public static IndexKey unpack(final byte[] src) {
        final int longCount = readLongCount(src);

        ByteBuffer buffer = TypeConvert.wrapBuffer(src);
        long[] fieldValues = new long[longCount - 1];
        for (int i = 0; i < fieldValues.length; ++i) {
            fieldValues[i] = buffer.getLong();
        }

        return new IndexKey(buffer.getLong(), fieldValues);
    }

    public static long unpackId(final byte[] src) {
        final int longCount = readLongCount(src);
        return TypeConvert.unpackLong(src, (longCount - 1) * ID_BYTE_SIZE);
    }

    public static KeyPattern buildKeyPattern(final long[] fieldValues) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * fieldValues.length);
        for (int i = 0; i < fieldValues.length; ++i) {
            buffer.putLong(fieldValues[i]);
        }
        return new KeyPattern(buffer.array());
    }

    public static KeyPattern buildKeyPattern(final long fieldValue) {
        return new KeyPattern(TypeConvert.pack(fieldValue));
    }

    private static int readLongCount(final byte[] src) {
        final int count = src.length / ID_BYTE_SIZE;
        final int tail = src.length % ID_BYTE_SIZE;
        if (count < 2 || tail != 0) {
            throw new KeyCorruptedException(src);
        }
        return count;
    }
}
