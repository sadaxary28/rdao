package com.infomaximum.rocksdb.core.struct;

import com.infomaximum.rocksdb.transaction.Transaction;

/**
 * Created by kris on 19.04.17.
 */
public abstract class DomainObject {

    private final long id;

    private Transaction transaction = null;

    public DomainObject(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean isReadOnly() {
        return (transaction==null || !transaction.isActive());
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

