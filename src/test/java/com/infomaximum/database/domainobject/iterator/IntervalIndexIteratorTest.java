package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.filter.IntervalIndexFilter;
import com.infomaximum.database.domainobject.filter.SortDirection;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class IntervalIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void longOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-4);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-9);
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
        assertValueEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -4L, 5L);
        assertValueEquals(Arrays.asList(-4L, -2L), StoreFileReadable.FIELD_SIZE, -4L, -1L);
        assertValueEquals(Arrays.asList(-2L, 0L, 3L), StoreFileReadable.FIELD_SIZE, -3L, 4L);
        assertValueEquals(Arrays.asList(3L), StoreFileReadable.FIELD_SIZE, 1L, 4L);
        assertValueEquals(Arrays.asList(), StoreFileReadable.FIELD_SIZE, 6L, 8L);
    }

    @Test
    public void dateOrderTest() throws Exception {
        final long currentTime = System.currentTimeMillis();

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setDate(new Date(currentTime));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDate(new Date(currentTime + 1000));
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDate(new Date(currentTime + 5 * 1000));
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(new Date(currentTime), new Date(currentTime + 1000)), StoreFileReadable.FIELD_DATE, new Date(currentTime), new Date(currentTime + 1000));
        assertValueEquals(Arrays.asList(new Date(currentTime + 1000), new Date(currentTime + 5 * 1000)), StoreFileReadable.FIELD_DATE, new Date(currentTime + 500), new Date(currentTime + 6 * 1000));
    }

    @Test
    public void doubleOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(5.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(3.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(2.0001);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(2.0000089);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-0.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(0.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-2.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-4.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(-9.0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.MIN_VALUE);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.MAX_VALUE);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.NEGATIVE_INFINITY);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.POSITIVE_INFINITY);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setDouble(Double.NaN);
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -5.0, 10.0);
        assertValueEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -4.0, 5.0);
        assertValueEquals(Arrays.asList(-4.0, -2.0), StoreFileReadable.FIELD_DOUBLE, -4.0, -1.0);
        assertValueEquals(Arrays.asList(-2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, -3.0, 4.0);
        assertValueEquals(Arrays.asList(2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, 2.0, 4.0);
        assertValueEquals(Arrays.asList(), StoreFileReadable.FIELD_DOUBLE, 6.0, 8.0);

        assertValueEquals(Arrays.asList(Double.MAX_VALUE), StoreFileReadable.FIELD_DOUBLE, 1000.0, Double.MAX_VALUE);
        assertValueEquals(Arrays.asList(Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN), StoreFileReadable.FIELD_DOUBLE, Double.MAX_VALUE, Double.NaN);
        assertValueEquals(Arrays.asList(Double.NEGATIVE_INFINITY, -9.0, -4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0, Double.MAX_VALUE, Double.POSITIVE_INFINITY),
                StoreFileReadable.FIELD_DOUBLE,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @Test
    public void stringGroupedOrderTest() throws Exception {
        final String name1 = "name1";
        final String name2 = "name2";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            obj.setFileName(name2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-2);
            obj.setFileName(name2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-4);
            obj.setFileName(name1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(-9);
            obj.setFileName(name2);
            transaction.save(obj);
        });

        assertValueEquals(Arrays.asList(-4L, 3L, 5L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name1));
        assertValueEquals(Arrays.asList(-2L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, -1L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name2));
        assertValueEquals(Arrays.asList(), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, -1L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, "name3"));
    }

    @Test
    public void longGroupedOrderTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            obj.setFolderId(1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFolderId(1);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(3);
            obj.setFolderId(2);
            transaction.save(obj);
        });

        assertIdEquals(Arrays.asList(2L, 1L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 1L));
        assertIdEquals(Arrays.asList(1L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, 5L, 5L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 1L));
        assertIdEquals(Arrays.asList(3L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 2L));
        assertIdEquals(Arrays.asList(), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 3L));
    }

    @Test
    public void duplicateTest() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(0);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(2);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setSize(5);
            transaction.save(obj);
        });

        assertIdEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L),
                new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L));

        assertIdEquals(Arrays.asList(1L, 2L, 3L),
                new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 3L));

        assertIdEquals(Arrays.asList(2L, 3L, 4L, 5L, 6L),
                new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, 2L, 5L));
    }

    protected void assertValueEquals(List<Double> expected, String fieldName, Double begin, Double end) throws DatabaseException {
        assertValueEquals(expected, new IntervalIndexFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Long> expected, String fieldName, Long begin, Long end) throws DatabaseException {
        assertValueEquals(expected, new IntervalIndexFilter(fieldName, begin, end));
    }

    protected void assertValueEquals(List<Date> expected, String fieldName, Date begin, Date end) throws DatabaseException {
        assertValueEquals(expected, new IntervalIndexFilter(fieldName, begin, end));
    }

    private <T> void assertValueEquals(List<T> expected, IntervalIndexFilter filter) throws DatabaseException {
        filter.setSortDirection(SortDirection.ASC);
        List<T> actual = getValues(filter);
        Assert.assertEquals(expected, actual);

        filter.setSortDirection(SortDirection.DESC);
        actual = getValues(filter);
        Collections.reverse(expected);
        Assert.assertEquals(expected, actual);
    }

    private void assertIdEquals(List<Long> expected, IntervalIndexFilter filter) throws DatabaseException {
        filter.setSortDirection(SortDirection.ASC);
        List<Long> actual = getIds(filter);
        Assert.assertEquals(expected, actual);

        filter.setSortDirection(SortDirection.DESC);
        actual = getIds(filter);
        Collections.reverse(expected);
        Assert.assertEquals(expected, actual);
    }

    private <T> List<T> getValues(IntervalIndexFilter filter) throws DatabaseException {
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter)) {
            List<T> result = new ArrayList<>();
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                result.add((T) storeFile.get(filter.getBeginValue().getClass(), filter.getIndexedFieldName()));
            }
            return result;
        }
    }

    private List<Long> getIds(IntervalIndexFilter filter) throws DatabaseException {
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter)) {
            List<Long> result = new ArrayList<>();
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                result.add(storeFile.getId());
            }
            return result;
        }
    }
}
