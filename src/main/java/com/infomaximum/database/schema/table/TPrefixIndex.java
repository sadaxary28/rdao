package com.infomaximum.database.schema.table;

import java.util.Arrays;

public class TPrefixIndex extends TBaseIndex {

    private final String[] fields;

    public TPrefixIndex(String[] fields) {
        if (fields.length == 0) {
            throw new IllegalArgumentException();
        }

        this.fields = fields;
    }

    public String[] getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TPrefixIndex that = (TPrefixIndex) o;
        return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
        return "PrefixIndex{" +
                "fields=" + Arrays.toString(fields) +
                '}';
    }
}
