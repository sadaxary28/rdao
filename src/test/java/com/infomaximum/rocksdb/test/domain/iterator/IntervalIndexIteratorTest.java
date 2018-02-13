package com.infomaximum.rocksdb.test.domain.iterator;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.filter.IntervalIndexFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
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

        assertEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -5L, 10L);
        assertEquals(Arrays.asList(-4L, -2L, 0L, 3L, 5L), StoreFileReadable.FIELD_SIZE, -4L, 5L);
        assertEquals(Arrays.asList(-4L, -2L), StoreFileReadable.FIELD_SIZE, -4L, -1L);
        assertEquals(Arrays.asList(-2L, 0L, 3L), StoreFileReadable.FIELD_SIZE, -3L, 4L);
        assertEquals(Arrays.asList(3L), StoreFileReadable.FIELD_SIZE, 1L, 4L);
        assertEquals(Arrays.asList(), StoreFileReadable.FIELD_SIZE, 6L, 8L);
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

        assertEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -5.0, 10.0);
        assertEquals(Arrays.asList(-4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0), StoreFileReadable.FIELD_DOUBLE, -4.0, 5.0);
        assertEquals(Arrays.asList(-4.0, -2.0), StoreFileReadable.FIELD_DOUBLE, -4.0, -1.0);
        assertEquals(Arrays.asList(-2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, -3.0, 4.0);
        assertEquals(Arrays.asList(2.0000089, 2.0001, 3.0), StoreFileReadable.FIELD_DOUBLE, 2.0, 4.0);
        assertEquals(Arrays.asList(), StoreFileReadable.FIELD_DOUBLE, 6.0, 8.0);

        assertEquals(Arrays.asList(Double.MAX_VALUE), StoreFileReadable.FIELD_DOUBLE, 1000.0, Double.MAX_VALUE);
        assertEquals(Arrays.asList(Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN), StoreFileReadable.FIELD_DOUBLE, Double.MAX_VALUE, Double.NaN);
        assertEquals(Arrays.asList(Double.NEGATIVE_INFINITY, -9.0, -4.0, -2.0, -0.0, 0.0, Double.MIN_VALUE, 2.0000089, 2.0001, 3.0, 5.0, Double.MAX_VALUE, Double.POSITIVE_INFINITY),
                StoreFileReadable.FIELD_DOUBLE,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    @Test
    public void groupedOrderTest() throws Exception {
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

        assertEquals(Arrays.asList(-4L, 3L, 5L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, 10L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name1));
        assertEquals(Arrays.asList(-2L), new IntervalIndexFilter(StoreFileReadable.FIELD_SIZE, -5L, -1L)
                .appendHashedField(StoreFileReadable.FIELD_FILE_NAME, name2));
    }

    protected void assertEquals(List<Double> expectedIds, String fieldName, Double begin, Double end) throws DatabaseException {
        assertEquals(expectedIds, new IntervalIndexFilter(fieldName, begin, end));
    }

    protected void assertEquals(List<Long> expectedIds, String fieldName, Long begin, Long end) throws DatabaseException {
        assertEquals(expectedIds, new IntervalIndexFilter(fieldName, begin, end));
    }

    private <T extends Number> void assertEquals(List<T> expectedIds, IntervalIndexFilter filter) throws DatabaseException {
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter)) {
            List<T> sizes = new ArrayList<>();
            while (iterator.hasNext()) {
                StoreFileReadable storeFile = iterator.next();

                sizes.add((T) storeFile.get(filter.getBeginValue().getClass(), filter.getIndexedFieldName()));
            }
            Assert.assertEquals(expectedIds, sizes);
        }
    }
}
