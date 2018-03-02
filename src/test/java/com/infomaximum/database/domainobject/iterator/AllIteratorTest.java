package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.exception.UnexpectedEndObjectException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class AllIteratorTest extends StoreFileDataTest {

    @Test
    public void checkInnerStructure() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }

        try (IteratorEntity iterator = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE, Collections.singleton(StoreFileReadable.FIELD_SIZE))) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            Assert.fail();
        } catch (UnexpectedEndObjectException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void orderingIterate() throws Exception {
        try (IteratorEntity iteratorEmpty = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
            Assert.assertFalse(iteratorEmpty.hasNext());
            try {
                iteratorEmpty.next();
                Assert.fail();
            } catch (NoSuchElementException e) {
            }
        }

        final int insertedRecordCount = 10;
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < insertedRecordCount; i++) {
                transaction.save(transaction.create(StoreFileEditable.class));
            }
        });

        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                if (prevId == storeFile.getId()) Assert.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assert.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void loadTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Field fieldValuesField = DomainObject.class.getDeclaredField("loadedFieldValues");
        fieldValuesField.setAccessible(true);

        Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE, loadingFields)) {
            int iteratedRecordCount = 0;
            while (iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                ConcurrentMap<String, Optional<Object>> fieldValues = (ConcurrentMap<String, Optional<Object>>) fieldValuesField.get(storeFile);
                Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_FILE_NAME));
                Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_SIZE));
                Assert.assertEquals(loadingFields.size(), fieldValues.size());
                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Field fieldValuesField = DomainObject.class.getDeclaredField("loadedFieldValues");
        fieldValuesField.setAccessible(true);

        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
            int iteratedRecordCount = 0;
            while (iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                ConcurrentMap<String, Optional<Object>> fieldValues = (ConcurrentMap<String, Optional<Object>>)fieldValuesField.get(storeFile);
                Assert.assertEquals(0, fieldValues.size());
                ++iteratedRecordCount;
            }

            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void iterateTransactional() throws Exception {
        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            // insert
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setSize(10);
            obj.setFormat(FormatType.B);
            transaction.save(obj);

            Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, obj.getId()).getSize());

            // change
            obj.setSize(20);
            transaction.save(obj);

            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertEquals(20L, i.next().getSize());
            }

            // change
            obj.setFormat(null);
            transaction.save(obj);

            StoreFileReadable storedObj = transaction.get(StoreFileReadable.class, obj.getId());
            Assert.assertNull(storedObj.getFormat());

            transaction.commit();
        }
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                transaction.save(obj);
            }
        });
    }
}
