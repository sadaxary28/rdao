package com.infomaximum.database.core.transaction.modifier;

import java.io.Serializable;

/**
 * Created by kris on 18.05.17.
 */
public abstract class Modifier implements Serializable {

    public final String columnFamily;
    private final byte[] key;

    public Modifier(String columnFamily, final byte[] key) {
        this.columnFamily = columnFamily;
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }
}
