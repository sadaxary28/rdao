package com.infomaximum.database.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.infomaximum.database.core.schema.TypeConverter;
import com.infomaximum.database.exception.runtime.IllegalTypeException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class TypeConvert {

    public static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public static ByteBuffer allocateBuffer(int capacity) {
        return ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
    }

    public static ByteBuffer wrapBuffer(final byte[] src) {
        return ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
    }

    public static String unpackString(byte[] value){
        return value != null ? new String(value, TypeConvert.CHARSET) : null;
    }

    public static String unpackString(byte[] value, int offset, int length){
        return !ByteUtils.isNullOrEmpty(value) ? new String(value, offset, length, TypeConvert.CHARSET) : null;
    }

    public static Integer unpackInteger(byte[] value) {
        return !ByteUtils.isNullOrEmpty(value) ? unpackInt(value) : null;
    }

    public static int unpackInt(byte[] value){
        return Ints.fromByteArray(value);
    }

    public static Long unpackLong(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? unpackLong(value, 0) : null;
    }

    public static long unpackLong(byte[] value, int offset){
        return Longs.fromBytes(value[0 + offset], value[1 + offset], value[2 + offset], value[3 + offset], value[4 + offset], value[5 + offset], value[6 + offset], value[7 + offset]);
    }

    public static Double unpackDouble(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? unpackDoublePrim(value) : null;
    }

    public static double unpackDoublePrim(byte[] value) {
        return Double.longBitsToDouble(unpackLong(value, 0));
    }

    public static Boolean unpackBoolean(byte[] value){
        return !ByteUtils.isNullOrEmpty(value) ? value[0] == (byte) 1 : null;
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

    public static byte[] pack(double value) {
        return pack(Double.doubleToRawLongBits(value));
    }

    public static byte[] pack(Double value){
        return value != null ? pack(value.doubleValue()) : EMPTY_BYTE_ARRAY;
    }

    public static byte[] pack(Date value){
        return value != null ? pack(value.getTime()) : EMPTY_BYTE_ARRAY;
    }

    public static <T> T unpack(Class<T> type, byte[] value, TypeConverter<T> packer) {
        if (packer != null) {
            return packer.unpack(value);
        } else if (type == String.class) {
            return (T) unpackString(value);
        } else if (type == Long.class) {
            return (T) unpackLong(value);
        } else if (type == Boolean.class) {
            return (T) unpackBoolean(value);
        } else if (type == Date.class) {
            return (T) unpackDate(value);
        } else if (type == Integer.class) {
            return (T) unpackInteger(value);
        } else if (type == Double.class) {
            return (T) unpackDouble(value);
        } else if (type == byte[].class) {
            return (T) value;
        }
        throw new IllegalTypeException("Unsupported type " + type);
    }

    public static <T> byte[] pack(Class<T> type, Object value, TypeConverter<T> converter){
        if (converter != null) {
            return converter.pack((T) value);
        } else if (type == String.class) {
            return pack((String) value);
        } else if (type == Long.class) {
            return pack((Long) value);
        } else if (type == Boolean.class) {
            return pack((Boolean) value);
        } else if (type == Date.class) {
            return pack((Date) value);
        } else if (type == Integer.class) {
            return pack((Integer) value);
        } else if (type == Double.class) {
            return pack((Double) value);
        } else if (type == byte[].class) {
            return (byte[]) value;
        }
        throw new IllegalTypeException("Unsupported type: " + type);
    }
}
