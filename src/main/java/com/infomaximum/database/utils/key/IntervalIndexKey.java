package com.infomaximum.database.utils.key;

import com.infomaximum.database.utils.IntervalIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class IntervalIndexKey extends BaseIntervalIndexKey {

    public IntervalIndexKey(long id, final long[] hashedValues) {
        super(id, hashedValues);
    }

    public void setIndexedValue(Object value) {
        indexedValue = IntervalIndexUtils.castToLong(value);
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * (hashedValues.length + 2) + Byte.BYTES);
        fillBuffer(hashedValues, indexedValue, buffer);
        buffer.putLong(getId());
        return buffer.array();
    }

    public static long unpackIndexedValue(final byte[] src) {
        return TypeConvert.unpackLong(src, src.length - 2 * ID_BYTE_SIZE);
    }
}
