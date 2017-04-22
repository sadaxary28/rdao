package com.infomaximum.rocksdb.utils;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

/**
 * Created by kris on 23.03.17.
 */
public class TypeConvertRocksdb {

    public static String getString(byte[] value){
        if (value==null) {
            return null;
        } else {
            return new String(value);
        }
    }

    public static Integer getInteger(byte[] value){
        if (value==null) {
            return null;
        } else {
            return Ints.fromByteArray(value);
        }
    }

    public static Long getLong(byte[] value){
        if (value==null) {
            return null;
        } else {
            return Longs.fromByteArray(value);
        }
    }

    public static Boolean getBoolean(byte[] value){
        if (value==null) {
            return null;
        } else {
            return (value[0]==(byte)1);
        }
    }

    public static byte[] pack(String value){
        return value.getBytes();
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
}
