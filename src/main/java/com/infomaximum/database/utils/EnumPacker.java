package com.infomaximum.database.utils;

import com.google.common.primitives.Ints;
import com.infomaximum.database.core.schema.TypePacker;

public abstract class EnumPacker<T extends Enum<?> & BaseEnum> implements TypePacker<T> {

    private final T[] enumConstants;

    protected EnumPacker(Class<T> clazz) {
        this.enumConstants = clazz.getEnumConstants();
    }

    @Override
    public byte[] pack(T value) {
        return value != null ? TypeConvert.pack(value.intValue()) : TypeConvert.EMPTY_BYTE_ARRAY;
    }

    @Override
    public T unpack(byte[] value) {
        if (ByteUtils.isNullOrEmpty(value)) {
            return null;
        }

        int enumValue = Ints.fromByteArray(value);
        for(T e : enumConstants) {
            if(enumValue == e.intValue()) {
                return e;
            }
        }
        throw new RuntimeException("Not found enum value " + enumValue + " into " + getClass());
    }
}
