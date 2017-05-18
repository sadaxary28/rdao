package com.infomaximum.rocksdb.transaction.struct.modifier;

/**
 * Created by kris on 18.05.17.
 */
public abstract class Modifier {

    public final String columnFamily;
    public final String key;

    public Modifier(String columnFamily, String key) {
        this.columnFamily = columnFamily;
        this.key = key;
    }
}
