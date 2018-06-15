package com.infomaximum.database.utils.key;

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
        for (int i = 0; i < hashedValues.length; ++i) {
            buffer.putLong(hashedValues[i]);
        }
        buffer.put(getSignByte(indexedValue));
        buffer.putLong(indexedValue);
        // for segment sorting
        if (isEndOfRange) {
            buffer.put(END_OF_RANGE_VALUE);
            buffer.putLong(0);
        } else {
            buffer.put(beginRangeValue < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE);
            buffer.putLong(beginRangeValue);
        }

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
}
