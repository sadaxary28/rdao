package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Date;

public class IntervalIndexKey  extends Key {

    private static final Byte NEGATIVE_VALUE = 0;
    private static final Byte POSITIVE_VALUE = 1;

    private final long[] hashedValues;
    private long indexedValue;

    public IntervalIndexKey(long id, final long[] hashedValues) {
        super(id);
        if (hashedValues == null) {
            throw new IllegalArgumentException();
        }
        this.hashedValues = hashedValues;
    }

    public long[] getHashedValues() {
        return hashedValues;
    }

    public void setIndexedValue(Date value) {
        indexedValue = castToLong(value);
    }

    public void setIndexedValue(Number value) {
        indexedValue = castToLong(value);
    }

    public void setIndexedValue(Object value) {
        indexedValue = castToLong(value);
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * (hashedValues.length + 2) + Byte.BYTES);
        for (int i = 0; i < hashedValues.length; ++i) {
            buffer.putLong(hashedValues[i]);
        }
        buffer.put(getSignByte(indexedValue));
        buffer.putLong(indexedValue);
        buffer.putLong(getId());
        return buffer.array();
    }

    public static long unpackId(final byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE);
    }

    public static int compare(byte[] key, long indexedValue) {
        long val = TypeConvert.unpackLong(key, key.length - 2 * ID_BYTE_SIZE);
        return Long.compare(val, indexedValue);
    }

    public static KeyPattern buildKeyPattern(final long[] hashedValues, Object beginValue) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * (hashedValues.length + 1) + Byte.BYTES);
        for (int i = 0; i < hashedValues.length; ++i) {
            buffer.putLong(hashedValues[i]);
        }
        final long value = castToLong(beginValue);
        buffer.put(getSignByte(value));
        buffer.putLong(value);
        return new KeyPattern(buffer.array(), false);
    }

    private static Byte getSignByte(long value) {
        return value < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE;
    }

    public static long castToLong(double value) {
        long val = Double.doubleToRawLongBits(value);
        return val < 0 ? 0x8000000000000000L - val : val;
    }

    public static long castToLong(Date value) {
        return value.getTime();
    }

    public static long castToLong(Object value) {
        if (value == null) {
            return 0;
        }

        if (value.getClass() == Long.class) {
            return castToLong(((Long) value).longValue());
        } else if (value.getClass() == Date.class) {
            return castToLong((Date) value);
        } else if (value.getClass() == Double.class) {
            return castToLong(((Double) value).doubleValue());
        }

        throw new IllegalTypeException("Unsupported type " + value.getClass());
    }
}
