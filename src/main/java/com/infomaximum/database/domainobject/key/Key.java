package com.infomaximum.database.domainobject.key;

public abstract class Key {

    public static final int ID_BYTE_SIZE = 8;

    private long id;

    public Key(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public abstract byte[] pack();
}
