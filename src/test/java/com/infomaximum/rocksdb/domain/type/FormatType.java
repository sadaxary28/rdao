package com.infomaximum.rocksdb.domain.type;

import com.infomaximum.rocksdb.core.struct.enums.PersistentEnumId;

/**
 * Created by kris on 16.06.17.
 */
public enum FormatType implements PersistentEnumId {

    A(1),

    B(2);

    private final int id;

    FormatType(int id) {
        this.id = id;
    }

    @Override
    public int getId() {
        return id;
    }
}
