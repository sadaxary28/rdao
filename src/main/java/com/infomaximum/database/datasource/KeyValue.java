package com.infomaximum.database.datasource;

import java.io.Serializable;

public class KeyValue implements Serializable {

    private final byte[] key;
    private final byte[] value;

    public KeyValue(final byte[] key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
