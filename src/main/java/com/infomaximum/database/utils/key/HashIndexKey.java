package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.exception.runtime.KeyCorruptedException;
import com.infomaximum.database.schema.HashIndex;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

import static com.infomaximum.database.schema.BaseIndex.ATTENDANT_BYTE_SIZE;

public class HashIndexKey extends IndexKey {

    private final long[] fieldValues;

    public HashIndexKey(long id, final HashIndex index) {
        this(id, index.attendant, new long[index.sortedFields.size()]);
    }

    public HashIndexKey(long id, final byte[] attendant, final long[] fieldValues) {
        super(id, attendant);

        if (fieldValues == null || fieldValues.length == 0) {
            throw new IllegalArgumentException();
        }
        this.fieldValues = fieldValues;
    }

    public long[] getFieldValues() {
        return fieldValues;
    }

    public byte[] getAttendant() {
        return attendant;
    }

    @Override
    public byte[] pack() {
        byte[] buffer = KeyUtils.allocateAndPutIndexAttendant(ATTENDANT_BYTE_SIZE + ID_BYTE_SIZE * fieldValues.length + ID_BYTE_SIZE,
                attendant);
        int offset = TypeConvert.pack(fieldValues, buffer, ATTENDANT_BYTE_SIZE);
        TypeConvert.pack(getId(), buffer, offset);
        return buffer;
    }

    public static HashIndexKey unpack(final byte[] src) {
        final int longCount = readLongCount(src);

        ByteBuffer buffer = TypeConvert.wrapBuffer(src);
        byte[] attendant = new byte[ATTENDANT_BYTE_SIZE];
        buffer.get(attendant);

        long[] fieldValues = new long[longCount - 1];
        for (int i = 0; i < fieldValues.length; ++i) {
            fieldValues[i] = buffer.getLong();
        }
        return new HashIndexKey(buffer.getLong(), attendant, fieldValues);
    }

    public static long unpackId(final byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE);
    }

    public static long unpackFirstIndexedValue(final byte[] src) {
        return TypeConvert.unpackLong(src, ATTENDANT_BYTE_SIZE);
    }

    public static KeyPattern buildKeyPattern(final HashIndex hashIndex, final long[] fieldValues) {
        byte[] buffer = KeyUtils.allocateAndPutIndexAttendant(ATTENDANT_BYTE_SIZE + ID_BYTE_SIZE * fieldValues.length,
                hashIndex.attendant);
        TypeConvert.pack(fieldValues, buffer, ATTENDANT_BYTE_SIZE);
        return new KeyPattern(buffer);
    }

    public static KeyPattern buildKeyPattern(final HashIndex hashIndex, final long fieldValue) {
        byte[] buffer = KeyUtils.allocateAndPutIndexAttendant(ATTENDANT_BYTE_SIZE + ID_BYTE_SIZE,
                hashIndex.attendant);
        TypeConvert.pack(fieldValue, buffer, ATTENDANT_BYTE_SIZE);
        return new KeyPattern(buffer);
    }

    public static KeyPattern buildKeyPatternForLastKey(final HashIndex hashIndex) {
        byte[] buffer = KeyUtils.allocateAndPutIndexAttendant(ATTENDANT_BYTE_SIZE + ID_BYTE_SIZE,
                hashIndex.attendant);
        TypeConvert.pack(0xFFFFFFFFFFFFFFFFL, buffer, ATTENDANT_BYTE_SIZE);
        return new KeyPattern(buffer, ATTENDANT_BYTE_SIZE);
    }

    private static int readLongCount(final byte[] src) {
        final int fieldsByteSize = src.length - ATTENDANT_BYTE_SIZE;
        final int count = fieldsByteSize / ID_BYTE_SIZE;
        final int tail = fieldsByteSize % ID_BYTE_SIZE;
        if (count < 2 || tail != 0) {
            throw new KeyCorruptedException(src);
        }
        return count;
    }
}
