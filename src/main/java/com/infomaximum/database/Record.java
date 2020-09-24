package com.infomaximum.database;

import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return id == record.id &&
                Arrays.equals(values, record.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id=" + id +
                ", values=" + Arrays.toString(values) +
                '}';
    }
}
