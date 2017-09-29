package com.infomaximum.database.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.infomaximum.database.core.schema.TypePacker;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Date;

/**
 * Created by kris on 23.03.17.
 */
public class TypeConvert {

    public static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final Charset CHARSET = Charset.forName("UTF-8");

    public static ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
    }

    public static ByteBuffer wrapBuffer(final byte[] src) {
        return ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
    }

    public static String unpackString(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? new String(value, TypeConvert.CHARSET) : null;
    }

    public static String unpackString(byte[] value, int offset, int length){
        return !ByteUtils.isNullOrEmpty(value) ? new String(value, offset, length, TypeConvert.CHARSET) : null;
    }

    public static Integer unpackInteger(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? Ints.fromByteArray(value) : null;
    }

    public static Long unpackLong(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? Longs.fromByteArray(value) : null;
    }

    public static Boolean unpackBoolean(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? Boolean.valueOf(value[0] == (byte)1) : null;
    }

    public static Date unpackDate(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? new Date(Longs.fromByteArray(value)) : null;
    }

       public static byte[] pack(String value){
        return value != null ? value.getBytes(TypeConvert.CHARSET) : EMPTY_BYTE_ARRAY;
    }

    public static byte[] pack(int value){
        return Ints.toByteArray(value);
    }

    public static byte[] pack(Integer value){
        return value != null ? pack(value.intValue()) : EMPTY_BYTE_ARRAY;
    }

    public static byte[] pack(long value){
        return Longs.toByteArray(value);
    }

    public static byte[] pack(Long value){
        return value != null ? pack(value.longValue()) : EMPTY_BYTE_ARRAY;
    }

    public static byte[] pack(boolean value){
        return new byte[] { value ? (byte)1 :(byte)0 };
    }

    public static byte[] pack(Boolean value){
        return value != null ? pack(value.booleanValue()) : EMPTY_BYTE_ARRAY;
    }

    public static byte[] pack(Date value){
        return value != null ? pack(value.getTime()) : EMPTY_BYTE_ARRAY;
    }

    public static <T> Object unpack(Class<T> type, byte[] value, TypePacker<T> packer){
        if (packer != null) {
            return packer.unpack(value);
        } else if (type == String.class) {
            return unpackString(value);
        } else if (type == Long.class) {
            return unpackLong(value);
        } else if (type == Integer.class) {
            return unpackInteger(value);
        } else if (type == Boolean.class) {
            return unpackBoolean(value);
        } else if (type == byte[].class) {
            return value;
        } else if (type == Date.class) {
            return unpackDate(value);
        }
        throw new RuntimeException("Unsupported type " + type);
    }

    public static <T> byte[] pack(Class<T> type, Object value, TypePacker<T> packer){
        if (packer != null) {
            return packer.pack((T) value);
        } else if (type == String.class) {
            return pack((String) value);
        } else if (type == Long.class) {
            return pack((Long) value);
        } else if (type == Integer.class) {
            return pack((Integer) value);
        } else if (type == Boolean.class) {
            return pack((Boolean) value);
        } else if (type == byte[].class) {
            return (byte[]) value;
        } else if (type == Date.class) {
            return pack((Date) value);
        }
        throw new RuntimeException("Not support type: " + type);
    }
}
