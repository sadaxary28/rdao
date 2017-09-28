package com.infomaximum.rocksdb.test.domain.iterator;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class IteratorFindEntityTest extends RocksDataTest {

    @Test
    public void ignoreCaseFind() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            final long sizeExpected = 10;
            final String nameExpected = "привет всем";
            
            domainObjectSource.createEntity(StoreFileReadable.class);
            domainObjectSource.getEngineTransaction().execute(transaction -> {
                StoreFileEditable obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setFileName("привет всем");
                obj.setSize(sizeExpected);
                domainObjectSource.save(obj, transaction);

                obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setFileName("привет");
                obj.setSize(sizeExpected);
                domainObjectSource.save(obj, transaction);

                obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setFileName("ПРИВЕТ ВСЕМ");
                obj.setSize(sizeExpected);
                domainObjectSource.save(obj, transaction);

                obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setFileName("всем");
                obj.setSize(sizeExpected);
                domainObjectSource.save(obj, transaction);

                obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setFileName("прИВет всЕм");
                obj.setSize(sizeExpected);
                domainObjectSource.save(obj, transaction);
            });

            Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
            Map<String, Object> filter = new HashMap<String, Object>(){{
                put(StoreFileReadable.FIELD_SIZE, sizeExpected);
                put(StoreFileReadable.FIELD_FILE_NAME, nameExpected);
            }};
            try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.findAll(StoreFileReadable.class, loadingFields, filter)) {
                int iteratedRecordCount = 0;
                while (iterator.hasNext()) {
                    StoreFileReadable storeFile = iterator.next();

                    Assert.assertTrue(storeFile.getFileName().equalsIgnoreCase(nameExpected));
                    ++iteratedRecordCount;
                }
                Assert.assertEquals(3, iteratedRecordCount);
            }
        }
    }

    @Test
    public void loadTwoFields() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            initAndFillStoreFiles(domainObjectSource, 100);

            Field fieldValuesField = DomainObject.class.getDeclaredField("fieldValues");
            fieldValuesField.setAccessible(true);

            Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
            Map<String, Object> filter = new HashMap<String, Object>(){{put(StoreFileReadable.FIELD_SIZE, 9L);}};
            try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.findAll(StoreFileReadable.class, loadingFields, filter)) {
                int iteratedRecordCount = 0;
                while (iterator.hasNext()) {
                    StoreFileReadable storeFile = iterator.next();

                    ConcurrentMap<String, Optional<Object>> fieldValues = (ConcurrentMap<String, Optional<Object>>)fieldValuesField.get(storeFile);
                    Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_FILE_NAME));
                    Assert.assertTrue(fieldValues.containsKey(StoreFileReadable.FIELD_SIZE));
                    Assert.assertEquals(loadingFields.size(), fieldValues.size());
                    ++iteratedRecordCount;
                }
                Assert.assertEquals(10, iteratedRecordCount);
            }
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            initAndFillStoreFiles(domainObjectSource, 100);

            Field fieldValuesField = DomainObject.class.getDeclaredField("fieldValues");
            fieldValuesField.setAccessible(true);

            Map<String, Object> filter = new HashMap<String, Object>(){{put(StoreFileReadable.FIELD_SIZE, 9L);}};
            try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.findAll(StoreFileReadable.class, filter)) {
                int iteratedRecordCount = 0;
                while (iterator.hasNext()) {
                    StoreFileReadable storeFile = iterator.next();

                    ConcurrentMap<String, Optional<Object>> fieldValues = (ConcurrentMap<String, Optional<Object>>)fieldValuesField.get(storeFile);
                    Assert.assertEquals(0, fieldValues.size());
                    ++iteratedRecordCount;
                }

                Assert.assertEquals(10, iteratedRecordCount);
            }
        }
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws DatabaseException {
        domainObjectSource.createEntity(StoreFileReadable.class);
        domainObjectSource.getEngineTransaction().execute(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = domainObjectSource.create(StoreFileEditable.class);
                obj.setSize(i % 10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                domainObjectSource.save(obj, transaction);
            }
        });
    }
}
