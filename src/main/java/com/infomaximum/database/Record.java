package com.infomaximum.database;

public class Record {

    private long id;
    private Object[] values;

    public Record(long id, Object[] values) {
        this.id = id;
        this.values = values;
    }

    public long getId() {
        return id;
    }

    void setId(long id) {
        this.id = id;
    }

    public Object[] getValues() {
        return values;
    }
}
