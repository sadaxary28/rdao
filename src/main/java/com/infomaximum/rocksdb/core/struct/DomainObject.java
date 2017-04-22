package com.infomaximum.rocksdb.core.struct;

/**
 * Created by kris on 19.04.17.
 */
public abstract class DomainObject {

    private final long id;

    private boolean readOnly=true;

    public DomainObject(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(getClass().getSuperclass().getName()).append('(')
                .append("id: ").append(id)
                .append(')').toString();
    }

    public void save(){}
}

