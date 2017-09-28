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
    public void loadTwoFields() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

            initAndFillStoreFiles(domainObjectSource, 100);

            Field fieldValuesField = DomainObject.class.getDeclaredField("fieldValues");
            fieldValuesField.setAccessible(true);

            Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
            Map<String, Object> filter = new HashMap<String, Object>(){{put(StoreFileReadable.FIELD_SIZE, 9L);}};
            try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.findAll(StoreFileReadable.class, loadingFields, filter)) {
                int iteratedRecordCount = 0;
                while (iStoreFileReadable.hasNext()) {
                    StoreFileReadable storeFile = iStoreFileReadable.next();

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
            try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.findAll(StoreFileReadable.class, filter)) {
                int iteratedRecordCount = 0;
                while (iStoreFileReadable.hasNext()) {
                    StoreFileReadable storeFile = iStoreFileReadable.next();

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
