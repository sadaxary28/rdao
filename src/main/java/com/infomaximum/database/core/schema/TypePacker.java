package com.infomaximum.database.core.schema;

public interface TypePacker<T> {

    byte[] pack(T value);
    T unpack(byte[] value);
}
