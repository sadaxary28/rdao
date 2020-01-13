package com.infomaximum.database.schema.table;

import java.util.Arrays;

public class THashIndex extends TBaseIndex {

    private final String[] fields;

    public THashIndex(String... fields) {
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
        THashIndex hashIndex = (THashIndex) o;
        return Arrays.equals(fields, hashIndex.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    @Override
    public String toString() {
        return "HashIndex{" +
                "fields=" + Arrays.toString(fields) +
                '}';
    }
}