package com.infomaximum.database.schema.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Table {

    private final String name;
    private final String namespace;
    private final List<TField> fields;
    private final List<THashIndex> hashIndexes;
    private final List<TPrefixIndex> prefixIndexes;
    private final List<TIntervalIndex> intervalIndexes;
    private final List<TRangeIndex> rangeIndexes;

    public Table(String name, String namespace, List<TField> fields) {
        this(name, namespace, fields, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public Table(String name, String namespace, List<TField> fields, List<THashIndex> hashIndexes) {
        this(name, namespace, fields, hashIndexes, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public Table(String name, String namespace, List<TField> fields,
                 List<THashIndex> hashIndexes, List<TPrefixIndex> prefixIndexes, List<TIntervalIndex> intervalIndexes, List<TRangeIndex> rangeIndexes) {
        this.name = name;
        this.namespace = namespace;
        this.fields = fields;
        this.hashIndexes = hashIndexes;
        this.prefixIndexes = prefixIndexes;
        this.intervalIndexes = intervalIndexes;
        this.rangeIndexes = rangeIndexes;
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public List<TField> getFields() {
        return fields;
    }

    public List<THashIndex> getHashIndexes() {
        return hashIndexes;
    }

    public List<TPrefixIndex> getPrefixIndexes() {
        return prefixIndexes;
    }

    public List<TIntervalIndex> getIntervalIndexes() {
        return intervalIndexes;
    }

    public List<TRangeIndex> getRangeIndexes() {
        return rangeIndexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(name, table.name) &&
                Objects.equals(fields, table.fields) &&
                Objects.equals(hashIndexes, table.hashIndexes) &&
                Objects.equals(prefixIndexes, table.prefixIndexes) &&
                Objects.equals(intervalIndexes, table.intervalIndexes) &&
                Objects.equals(rangeIndexes, table.rangeIndexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fields, hashIndexes, prefixIndexes, intervalIndexes, rangeIndexes);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", fields=" + fields +
                ", hashIndexes=" + hashIndexes +
                ", prefixIndexes=" + prefixIndexes +
                ", intervalIndexes=" + intervalIndexes +
                ", rangeIndexes=" + rangeIndexes +
                '}';
    }
}
