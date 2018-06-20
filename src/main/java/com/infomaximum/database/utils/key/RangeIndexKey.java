package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class RangeIndexKey extends BaseIntervalIndexKey {

    private static final Byte END_OF_RANGE_VALUE = 0x00;
    private static final Byte NEGATIVE_VALUE = 0x1F;
    private static final Byte POSITIVE_VALUE = 0x7F;

    private long beginRangeValue;
    private boolean isEndOfRange;

    public RangeIndexKey(long id, long[] hashedValues) {
        super(id, hashedValues);
    }

    public void setIndexedValue(long value) {
        indexedValue = value;
    }

    public void setBeginRangeValue(long value) {
        beginRangeValue = value;
    }

    public void setEndOfRange(boolean endOfRange) {
        isEndOfRange = endOfRange;
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer((hashedValues.length + 1) * ID_BYTE_SIZE + 2 * (Long.BYTES + Byte.BYTES));
        fillBuffer(hashedValues, indexedValue, buffer);
        // for segment sorting
        putBeginRange(beginRangeValue, isEndOfRange, buffer);
        buffer.putLong(getId());
        return buffer.array();
    }

    public static long unpackIndexedValue(byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE - 2 * Long.BYTES - Byte.BYTES);
    }

    public static boolean unpackEndOfRange(byte[] src) {
        return src[src.length - ID_BYTE_SIZE - Long.BYTES - Byte.BYTES] == END_OF_RANGE_VALUE;
    }

    public static void setIndexedValue(long indexedValue, byte[] dstKey) {
        TypeConvert.pack(indexedValue, dstKey, dstKey.length - ID_BYTE_SIZE - 2 * Long.BYTES - Byte.BYTES);
    }

    public static KeyPattern buildBeginPattern(long[] hashedValues, long beginRangeValue) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * hashedValues.length + (Byte.BYTES + Long.BYTES) * 2);
        fillBuffer(hashedValues, beginRangeValue, buffer);
        putBeginRange(beginRangeValue, false, buffer);
        return new KeyPattern(buffer.array(), ID_BYTE_SIZE * hashedValues.length);
    }

    private static void putBeginRange(long beginRangeValue, boolean isEndOfRange, ByteBuffer destination) {
        if (isEndOfRange) {
            destination.put(END_OF_RANGE_VALUE);
            destination.putLong(0);
        } else {
            destination.put(beginRangeValue < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE);
            destination.putLong(beginRangeValue);
        }
    }
}
