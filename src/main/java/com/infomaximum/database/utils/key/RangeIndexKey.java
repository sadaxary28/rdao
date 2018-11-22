package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.BaseIntervalIndex;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;

public class RangeIndexKey extends BaseIntervalIndexKey {

    public enum Type {
        BEGIN, END, DOT
    }

    private static final byte END_OF_RANGE_VALUE = 0x00;
    private static final byte NEGATIVE_VALUE = 0x1F;
    private static final byte POSITIVE_VALUE = 0x7F;
    private static final byte DOT_VALUE = (byte) 0xEF;

    private long beginRangeValue;
    private Type type;

    public RangeIndexKey(long id, long[] hashedValues, BaseIntervalIndex index) {
        super(id, hashedValues, index);
    }

    public void setIndexedValue(long value) {
        indexedValue = value;
    }

    public void setBeginRangeValue(long value) {
        beginRangeValue = value;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public byte[] pack() {
        ByteBuffer buffer = TypeConvert.allocateBuffer((hashedValues.length + 1) * ID_BYTE_SIZE + 2 * (Long.BYTES + Byte.BYTES));
        BaseIntervalIndexKey.fillBuffer(hashedValues, indexedValue, buffer);
        // for segment sorting
        putBeginRange(beginRangeValue, type, buffer);
        buffer.putLong(getId());
        return buffer.array();
    }

    public static long unpackIndexedValue(byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE - 2 * Long.BYTES - Byte.BYTES);
    }

    public static Type unpackType(byte[] src) {
        switch (src[src.length - ID_BYTE_SIZE - Long.BYTES - Byte.BYTES]) {
            case END_OF_RANGE_VALUE:
                return Type.END;
            case DOT_VALUE:
                return Type.DOT;
            default:
                return Type.BEGIN;
        }
    }

    public static void setIndexedValue(long indexedValue, byte[] dstKey) {
        int offset = dstKey.length - ID_BYTE_SIZE - 2 * Long.BYTES - Byte.BYTES;
        dstKey[offset - 1] = BaseIntervalIndexKey.getSignByte(indexedValue);
        TypeConvert.pack(indexedValue, dstKey, offset);
    }

    public static void setType(Type type, byte[] dstKey) {
        int beginPos = dstKey.length - ID_BYTE_SIZE - Long.BYTES - Byte.BYTES;
        switch (type) {
            case BEGIN:
                long beginRangeValue = TypeConvert.unpackLong(dstKey, beginPos + 1);
                dstKey[beginPos] = beginRangeValue < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE;
                break;
            case END:
                dstKey[beginPos] = END_OF_RANGE_VALUE;
                break;
            case DOT:
                dstKey[beginPos] = DOT_VALUE;
                TypeConvert.pack(0, dstKey, beginPos + 1);
                break;
        }
    }

    public static KeyPattern buildBeginPattern(long[] hashedValues, long beginRangeValue) {
        ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE * hashedValues.length + (Byte.BYTES + Long.BYTES) * 2);
        BaseIntervalIndexKey.fillBuffer(hashedValues, beginRangeValue, buffer);
        putBeginRange(beginRangeValue, Type.BEGIN, buffer);
        return new KeyPattern(buffer.array(), ID_BYTE_SIZE * hashedValues.length);
    }

    private static void putBeginRange(long beginRangeValue, Type type, ByteBuffer destination) {
        switch (type) {
            case BEGIN:
                destination.put(beginRangeValue < 0 ? NEGATIVE_VALUE : POSITIVE_VALUE);
                destination.putLong(beginRangeValue);
                break;
            case END:
                destination.put(END_OF_RANGE_VALUE);
                destination.putLong(beginRangeValue);
                break;
            case DOT:
                destination.put(DOT_VALUE);
                destination.putLong(0);
                break;
        }
    }
}
