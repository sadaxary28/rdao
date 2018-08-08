package com.infomaximum.database.domainobject.filter;

import java.time.Instant;

public class IntervalFilter extends BaseIntervalFilter {

    private final Integer indexedFieldNumber;
    private SortDirection sortDirection = SortDirection.ASC;

    public IntervalFilter(Integer indexedFieldNumber, Double beginValue, Double endValue) {
        super(beginValue, endValue);

        this.indexedFieldNumber = indexedFieldNumber;
    }

    public IntervalFilter(Integer indexedFieldNumber, Long beginValue, Long endValue) {
        super(beginValue, endValue);

        this.indexedFieldNumber = indexedFieldNumber;
    }

    public IntervalFilter(Integer indexedFieldNumber, Instant beginValue, Instant endValue) {
        super(beginValue, endValue);

        this.indexedFieldNumber = indexedFieldNumber;
    }

    public Integer getIndexedFieldNumber() {
        return indexedFieldNumber;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }

    @Override
    public IntervalFilter appendHashedField(Integer number, Object value) {
        return (IntervalFilter) super.appendHashedField(number, value);
    }

    public IntervalFilter setSortDirection(SortDirection sortDirection) {
        if (sortDirection == null) {
            throw new IllegalArgumentException();
        }

        this.sortDirection = sortDirection;
        return this;
    }
}
