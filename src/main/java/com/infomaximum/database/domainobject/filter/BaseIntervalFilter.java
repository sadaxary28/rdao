package com.infomaximum.database.domainobject.filter;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseIntervalFilter implements Filter {

    private final Object beginValue;
    private final Object endValue;
    private Map<String, Object> values = null;

    BaseIntervalFilter(Double beginValue, Double endValue) {
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    BaseIntervalFilter(Long beginValue, Long endValue) {
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    BaseIntervalFilter(Instant beginValue, Instant endValue) {
        this.beginValue = beginValue;
        this.endValue = endValue;
    }

    public BaseIntervalFilter appendHashedField(String name, Object value) {
        if (values == null) {
            values = new HashMap<>();
        }
        values.put(name, value);
        return this;
    }

    public Map<String, Object> getHashedValues() {
        return values != null ? Collections.unmodifiableMap(values) : Collections.emptyMap();
    }

    public Object getBeginValue() {
        return beginValue;
    }

    public Object getEndValue() {
        return endValue;
    }
}
