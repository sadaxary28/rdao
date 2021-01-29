package com.infomaximum.database.domainobject.engine.index;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.domainobject.filter.SortDirection;
import com.infomaximum.database.exception.DatabaseException;
import org.assertj.core.api.Assertions;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IntervalTest extends StoreFileDataTest {
    protected void assertValueEquals(List<Double> expected, Integer fieldName, Double begin, Double end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Long> expected, Integer fieldName, Long begin, Long end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Instant> expected, Integer fieldName, Instant begin, Instant end) throws DatabaseException {
        assertValueEquals(expected, new IntervalFilter(fieldName, begin, end));
    }

    private <T> void assertValueEquals(List<T> expected, IntervalFilter filter) throws DatabaseException {
        filter.setSortDirection(SortDirection.ASC);
        List<T> actual = getValues(filter);
        Assertions.assertThat(actual).isEqualTo(actual);

        filter.setSortDirection(SortDirection.DESC);
        actual = getValues(filter);
        Collections.reverse(expected);
        Assertions.assertThat(actual).isEqualTo(actual);
    }

    private <T> List<T> getValues(IntervalFilter filter) throws DatabaseException {
        try (RecordIterator iterator = recordSource.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE, filter)){
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                result.add((T) storeFile.getValues()[filter.getIndexedFieldId()]);
            }
            return result;
        }
    }
}
