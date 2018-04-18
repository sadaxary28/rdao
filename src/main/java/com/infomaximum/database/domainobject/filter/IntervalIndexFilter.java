package com.infomaximum.database.domainobject.filter;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IntervalIndexFilter implements Filter {

    private final Map<String, Object> values = new HashMap<>();
    private final String indexedFieldName;
    private final Object beginValue;
    private final Object endValue;
    private SortDirection sortDirection = SortDirection.ASC;

    public IntervalIndexFilter(String indexedFieldName, Double beginValue, Double endValue) {
        this.indexedFieldName = indexedFieldName;
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    public IntervalIndexFilter(String indexedFieldName, Long beginValue, Long endValue) {
        this.indexedFieldName = indexedFieldName;
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    public IntervalIndexFilter(String indexedFieldName, Instant beginValue, Instant endValue) {
        this.indexedFieldName = indexedFieldName;
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    public IntervalIndexFilter appendHashedField(String name, Object value) {
        values.put(name, value);
        return this;
    }

    public Map<String, Object> getHashedValues() {
        return Collections.unmodifiableMap(values);
    }

    public String getIndexedFieldName() {
        return indexedFieldName;
    }

    public Object getBeginValue() {
        return beginValue;
    }

    public Object getEndValue() {
        return endValue;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }

    public IntervalIndexFilter setSortDirection(SortDirection sortDirection) {
        if (sortDirection == null) {
            throw new IllegalArgumentException();
        }

        this.sortDirection = sortDirection;
        return this;
    }
}
