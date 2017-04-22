package com.infomaximum.rocksdb.core.struct;

/**
 * Created by kris on 19.04.17.
 */
public abstract class DomainObject {

    private final long id;

    private boolean readOnly=false;

    public DomainObject(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public abstract void save();
}

