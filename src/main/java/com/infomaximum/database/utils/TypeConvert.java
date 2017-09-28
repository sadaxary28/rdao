package com.infomaximum.database.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.infomaximum.database.struct.enums.PersistentEnumId;

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
        if (value==null) {
            return null;
        } else {
            return new String(value, TypeConvert.CHARSET);
        }
    }

    public static String unpackString(byte[] value, int offset, int length){
        if (value==null) {
            return null;
        } else {
            return new String(value, offset, length, TypeConvert.CHARSET);
        }
    }

    public static Integer unpackInteger(byte[] value){
        if (value==null) {
            return null;
        } else {
            return Ints.fromByteArray(value);
        }
    }

    public static Long unpackLong(byte[] value){
        if (value==null) {
            return null;
        } else {
            return Longs.fromByteArray(value);
        }
    }

    public static Boolean unpackBoolean(byte[] value){
        if (value==null) {
            return null;
        } else {
            return (value[0]==(byte)1);
        }
    }

    public static Date unpackDate(byte[] value){
        if (value==null) {
            return null;
        } else {
            return new Date(unpackLong(value));
        }
    }


    public static byte[] pack(String value){
        return value.getBytes(TypeConvert.CHARSET);
    }

    public static byte[] pack(int value){
        return Ints.toByteArray(value);
    }

    public static byte[] pack(long value){
        return Longs.toByteArray(value);
    }

    public static byte[] pack(boolean value){
        return new byte[] { (value)? (byte)1 :(byte)0 };
    }

    public static byte[] pack(Date value){
        return pack(value.getTime());
    }

    public static Object unpack(Class<?> type, byte[] value){
        if (type==String.class) {
            return unpackString(value);
        } else if (type == Long.class || type == long.class) {
            return unpackLong(value);
        } else if (type == Integer.class || type == int.class) {
            return unpackInteger(value);
        } else if (type == Boolean.class || type == boolean.class) {
            return unpackBoolean(value);
        } else if (type == Byte[].class || type == byte[].class ) {
            return value;
        } else if (type == Date.class) {
            return unpackDate(value);
        } else if (type.isEnum()) {
            if (PersistentEnumId.class.isAssignableFrom(type)) {
                Integer idObj = unpackInteger(value);
                if (idObj == null) {
                    return null;
                }

                int id = idObj.intValue();
                for(PersistentEnumId iEnum : ((Class<? extends PersistentEnumId>)type).getEnumConstants()) {
                    if(id == iEnum.getId()) {
                        return iEnum;
                    }
                }
                throw new RuntimeException("not found enum: " + type + ", id: " + id);
            } else {
                String name = unpackString(value);
                return name != null ? Enum.valueOf((Class<? extends Enum>) type, name) : null;
            }
        } else {
            throw new RuntimeException("Not support type: " + type);
        }
    }

    public static byte[] pack(Class<?> type, Object value){
        if (type==String.class) {
            return pack((String) value);
        } else if (type == Long.class || type == long.class) {
            return pack((Long) value);
        } else if (type == Integer.class || type == int.class) {
            return pack((Integer) value);
        } else if (type == Boolean.class || type == boolean.class) {
            return pack((Boolean) value);
        } else if (type == byte[].class) {
            return (byte[]) value;
        } else if (type == Date.class) {
            return pack((Date) value);
        } else if (type.isEnum()) {
            if (PersistentEnumId.class.isAssignableFrom(type)) {
                return pack(((PersistentEnumId) value).getId());
            } else {
                return pack(((Enum)value).name());
            }
        } else {
            throw new RuntimeException("Not support type: " + type);
        }
    }
}
