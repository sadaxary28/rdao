package com.infomaximum.database.domainobject.filter;

import java.time.Instant;

public class RangeFilter extends BaseIntervalFilter {

    public static class IndexedField {

        public final String beginField;
        public final String endField;

        public IndexedField(String beginField, String endField) {
            this.beginField = beginField;
            this.endField = endField;
        }
    }

    private final IndexedField indexedField;

    public RangeFilter(IndexedField indexedField, Double beginValue, Double endValue) {
        super(beginValue, endValue);

        this.indexedField = indexedField;
    }

    public RangeFilter(IndexedField indexedField, Long beginValue, Long endValue) {
        super(beginValue, endValue);

        this.indexedField = indexedField;
    }

    public RangeFilter(IndexedField indexedField, Instant beginValue, Instant endValue) {
        super(beginValue, endValue);

        this.indexedField = indexedField;
    }

    @Override
    public RangeFilter appendHashedField(String name, Object value) {
        return (RangeFilter) super.appendHashedField(name, value);
    }

    public IndexedField getIndexedField() {
        return indexedField;
    }
}
