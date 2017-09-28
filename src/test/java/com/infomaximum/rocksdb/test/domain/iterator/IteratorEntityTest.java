package com.infomaximum.rocksdb.test.domain.iterator;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class IteratorEntityTest extends RocksDataTest {

    @Test
    public void orderingIterate() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
            domainObjectSource.createEntity(StoreFileReadable.class);

            try (IteratorEntity iteratorEmpty = domainObjectSource.iterator(StoreFileReadable.class, null)) {
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

            try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.iterator(StoreFileReadable.class, null)) {
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
    }

    @Test
    public void loadTwoFields() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            final int insertedRecordCount = 10;
            initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

            Field fieldValuesField = DomainObject.class.getDeclaredField("fieldValues");
            fieldValuesField.setAccessible(true);

            Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
            try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.iterator(StoreFileReadable.class, loadingFields)) {
                int iteratedRecordCount = 0;
                while (iStoreFileReadable.hasNext()) {
                    StoreFileReadable storeFile = iStoreFileReadable.next();

                    ConcurrentMap<String, Optional<Object>> fieldValues = (ConcurrentMap<String, Optional<Object>>)fieldValuesField.get(storeFile);
                    Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_FILE_NAME));
                    Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_SIZE));
                    Assert.assertEquals(loadingFields.size(), fieldValues.size());
                    ++iteratedRecordCount;
                }
                Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
            }
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            final int insertedRecordCount = 10;
            initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

            Field fieldValuesField = DomainObject.class.getDeclaredField("fieldValues");
            fieldValuesField.setAccessible(true);

            try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.iterator(StoreFileReadable.class, null)) {
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
    }

    @Test
    public void iterateTransactional() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
            domainObjectSource.createEntity(StoreFileReadable.class);

            try (Transaction transaction = domainObjectSource.buildTransaction()) {
                // insert
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFormat(FormatType.B);
                transaction.save(obj);

                Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, null, obj.getId()).getSize());

                // change
                obj.setSize(20);
                transaction.save(obj);

                try (IteratorEntity<StoreFileReadable> i = transaction.iterator(StoreFileReadable.class, null)) {
                    Assert.assertEquals(20L, i.next().getSize());
                }

                // change
                obj.setFormat(null);
                transaction.save(obj);

                StoreFileReadable storedObj = transaction.get(StoreFileReadable.class, null, obj.getId());
                Assert.assertNull(storedObj.getFormat());

                transaction.commit();
            }
        }
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws DatabaseException {
        domainObjectSource.createEntity(StoreFileReadable.class);
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
