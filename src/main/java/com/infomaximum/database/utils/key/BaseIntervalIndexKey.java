package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public abstract class BaseIntervalIndexKey extends Key {

    private static final byte NEGATIVE_VALUE = 0;
    private static final byte POSITIVE_VALUE = 1;

    final long[] hashedValues;
    long indexedValue;

    BaseIntervalIndexKey(long id, long[] hashedValues) {
        super(id);
        if (hashedValues == null) {
            throw new IllegalArgumentException();
        }
        this.hashedValues = hashedValues;
    }

    public long[] getHashedValues() {
        return hashedValues;
    }

    public static long unpackId(byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE);
    }

    public static KeyPattern buildLeftBorder(long[] hashedValues, long indexedValue) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * (hashedValues.length + 1) + Byte.BYTES);
        fillBuffer(hashedValues, indexedValue, buffer);
        return new KeyPattern(buffer.array(), ID_BYTE_SIZE * hashedValues.length);
    }

    public static KeyPattern buildRightBorder(long[] hashedValues, long indexedValue) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * (hashedValues.length + 2) + Byte.BYTES);
        fillBuffer(hashedValues, indexedValue, buffer);
        buffer.putLong(0xffffffffffffffffL);
        KeyPattern pattern = new KeyPattern(buffer.array(), ID_BYTE_SIZE * hashedValues.length);
        pattern.setForBackward(true);
        return pattern;
    }

    static void fillBuffer(long[] hashedValues, long indexedValue, ByteBuffer destination) {
        for (int i = 0; i < hashedValues.length; ++i) {
            destination.putLong(hashedValues[i]);
        }
        destination.put(getSignByte(indexedValue));
        destination.putLong(indexedValue);
    }

    static Byte getSignByte(long value) {
        return value < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE;
    }
}

