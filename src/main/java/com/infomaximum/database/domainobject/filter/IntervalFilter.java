package com.infomaximum.database.domainobject.filter;

import java.time.Instant;

public class IntervalFilter extends BaseIntervalFilter {

    private final String indexedFieldName;
    private SortDirection sortDirection = SortDirection.ASC;

    public IntervalFilter(String indexedFieldName, Double beginValue, Double endValue) {
        super(beginValue, endValue);

        this.indexedFieldName = indexedFieldName;
    }

    public IntervalFilter(String indexedFieldName, Long beginValue, Long endValue) {
        super(beginValue, endValue);

        this.indexedFieldName = indexedFieldName;
    }

    public IntervalFilter(String indexedFieldName, Instant beginValue, Instant endValue) {
        super(beginValue, endValue);

        this.indexedFieldName = indexedFieldName;
    }

    public String getIndexedFieldName() {
        return indexedFieldName;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }

    @Override
    public IntervalFilter appendHashedField(String name, Object value) {
        return (IntervalFilter) super.appendHashedField(name, value);
    }

    public IntervalFilter setSortDirection(SortDirection sortDirection) {
        if (sortDirection == null) {
            throw new IllegalArgumentException();
        }

        this.sortDirection = sortDirection;
        return this;
    }
}
