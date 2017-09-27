package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.exeption.runtime.KeyCorruptedException;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

/**
 * Created by kris on 27.04.17.
 */
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
        ByteBuffer buffer = TypeConvert.allocateBuffer(8 + 8 * fieldValues.length);
        for (int i = 0; i < fieldValues.length; ++i) {
            buffer.putLong(fieldValues[i]);
        }
        buffer.putLong(getId());
        return buffer.array();
    }

    public static IndexKey unpack(final byte[] src) {
        final int longCount = src.length / 8;
        final int tail = src.length % 8;
        if (longCount < 2 || tail != 0) {
            throw new KeyCorruptedException(src);
        }

        ByteBuffer buffer = TypeConvert.wrapBuffer(src);
        long[] fieldValues = new long[longCount - 1];
        for (int i = 0; i < fieldValues.length; ++i) {
            fieldValues[i] = buffer.getLong();
        }

        return new IndexKey(buffer.getLong(), fieldValues);
    }

    public static byte[] getKeyPrefix(final long[] fieldValues) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(8 * fieldValues.length);
        for (int i = 0; i < fieldValues.length; ++i) {
            buffer.putLong(fieldValues[i]);
        }
        return buffer.array();
    }
}
