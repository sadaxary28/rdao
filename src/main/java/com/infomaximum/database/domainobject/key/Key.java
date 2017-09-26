package com.infomaximum.database.domainobject.key;

public abstract class Key {

    private final long id;

    public Key(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public abstract byte[] pack();
}
