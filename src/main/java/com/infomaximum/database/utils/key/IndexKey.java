package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.exception.runtime.KeyCorruptedException;
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
        byte[] buffer = new byte[ID_BYTE_SIZE + ID_BYTE_SIZE * fieldValues.length];
        int offset = 0;
        for (int i = 0; i < fieldValues.length; ++i) {
            TypeConvert.pack(fieldValues[i], buffer, offset);
            offset += ID_BYTE_SIZE;
        }
        TypeConvert.pack(getId(), buffer, offset);
        return buffer;
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
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE);
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
