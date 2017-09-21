package com.infomaximum.database.core.transaction.struct.modifier;

import java.io.Serializable;

/**
 * Created by kris on 18.05.17.
 */
public abstract class Modifier implements Serializable {

    public final String columnFamily;
    public final String key;

    public Modifier(String columnFamily, String key) {
        this.columnFamily = columnFamily;
        this.key = key;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getClass().getSimpleName())
                .append(':').append(columnFamily).append(':')
                .append(key).toString();
    }
}
