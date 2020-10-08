package com.infomaximum.database.domainobject.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.utils.HashIndexUtils;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.*;

public class HashIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void ignoreCaseFind() throws Exception {

        final long sizeExpected = 10;
        final String nameExpected = "привет всем";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            obj.setSize(sizeExpected);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм");
            obj.setSize(sizeExpected);
            transaction.save(obj);
        });

        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, sizeExpected).appendField(StoreFileReadable.FIELD_FILE_NAME, nameExpected);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", filter)){
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();
                Assertions.assertThat(((String) storeFile.getValues()[StoreFileReadable.FIELD_FILE_NAME])).isEqualToIgnoringCase(nameExpected);

                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(3);
        }
    }

    @Test
    public void loadTwoFields() throws Exception {
        initAndFillStoreFiles(domainObjectSource, 100);
        List<Record> actual = new ArrayList<>();

        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, 9L);
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", filter)) {
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();
                actual.add(storeFile);
            }
        }
        Assertions.assertThat(actual).hasSize(10);
    }

    @Test
    public void loadNullableTwoFields() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < 10; i++) {
                transaction.save(transaction.create(StoreFileEditable.class));
            }
        });

        Field fieldValuesField = DomainObject.class.getDeclaredField("loadedFieldValues");
        fieldValuesField.setAccessible(true);

        HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, null);
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", filter)) {
            int iteratedRecordCount = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();
                Assertions.assertThat(storeFile.getValues()[StoreFileReadable.FIELD_SIZE]).isNull();

                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(10);
        }
    }

//    @Test
//    public void findTransactional() throws Exception {
//        try (Transaction transaction = domainObjectSource.buildTransaction()) {
//            // insert
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(10);
//            transaction.save(obj);
//
//            Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, obj.getId()).getSize());
//
//            // change
//            obj.setSize(20);
//            transaction.save(obj);
//
//            HashFilter filter = new HashFilter(StoreFileReadable.FIELD_SIZE, 20L);
//            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, filter)) {
//                Assert.assertTrue(i.hasNext());
//            }
//
//            transaction.commit();
//        }
//    }

    @Test
    public void findBySingleField() throws Exception {

        final String nameExpected = "привет всем";

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setSize(20);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setSize(30);
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ");
            obj.setSize(40);
            transaction.save(obj);
        });

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 50L))) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 30L))) {
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                ++count;
            }
            Assertions.assertThat(count).isEqualTo(1);
        }

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "unknown"))) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store",  new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameExpected))) {
            Assertions.assertThat(iterator.hasNext()).isTrue();
        }

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameExpected))) {
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                ++count;
            }
            Assertions.assertThat(count).isEqualTo(2);
        }
    }

    @Test
    public void testCollision() throws Exception {
        final int storedCount = 5;
        final String nameCol1 = "http://ccwf/login.aspx?login=auto&crid=20560116";
        final String nameCol2 = "http://ccwf/login.aspx?login=auto&crid=20517268";
        Assertions.assertThat(HashIndexUtils.buildHash(String.class, nameCol2, null))
                .isEqualTo(HashIndexUtils.buildHash(String.class, nameCol1, null));

        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < storedCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName(nameCol1);
                transaction.save(obj);
            }
        });

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameCol2))) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, nameCol1))) {
            int count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                ++count;
            }
            Assertions.assertThat(storedCount).isEqualTo(count);
        }
    }

//    @Test
//    public void iterateAndChange() throws Exception {
//        final long value = 20;
//
//        domainObjectSource.executeTransactional(transaction -> {
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("привет всем");
//            obj.setSize(value);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("привет");
//            obj.setSize(value);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setFileName("ПРИВЕТ ВСЕМ");
//            obj.setSize(40);
//            obj.setLocalBegin(LocalDateTime.of(2018, 10, 22, 18, 32));
//            transaction.save(obj);
//        });
//
//        try (Transaction transaction = domainObjectSource.buildTransaction()) {
//            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, value))) {
//                List<Long> ids = new ArrayList<>();
//                while (i.hasNext()) {
//                    StoreFileEditable s = transaction.get(StoreFileEditable.class, 3);
//                    s.setSize(value);
//                    transaction.save(s);
//
//                    StoreFileReadable item = i.next();
//                    Assert.assertEquals(value, item.getSize());
//
//                    ids.add(item.getId());
//                }
//
//                Assert.assertEquals(Arrays.asList(1L, 2L), ids);
//            }
//
//            Assert.assertNotNull(transaction.find(
//                    StoreFileReadable.class,
//                    new HashFilter(StoreFileReadable.FIELD_LOCAL_BEGIN, LocalDateTime.of(2018, 10, 22, 18, 32))
//            ));
//        }
//    }
//
//    @Test
//    public void removeAndFind() throws Exception {
//        domainObjectSource.executeTransactional(transaction -> {
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(1);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(20);
//            transaction.save(obj);
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(1);
//            transaction.save(obj);
//        });
//
//        domainObjectSource.executeTransactional(transaction -> {
//            transaction.remove(transaction.get(StoreFileEditable.class, 1));
//            transaction.remove(transaction.get(StoreFileEditable.class, 2));
//
//            testFind(transaction, new HashFilter(StoreFileReadable.FIELD_SIZE, 20L));
//            testFind(transaction, new HashFilter(StoreFileReadable.FIELD_SIZE, 1L), 3);
//        });
//
//        testFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 20L));
//        testFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 1L), 3);
//
//        StoreFileEditable[] newObj = {null};
//        domainObjectSource.executeTransactional(transaction -> {
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(1);
//            transaction.save(obj);
//
//            try (IteratorEntity<StoreFileEditable> i = transaction.find(StoreFileEditable.class, new HashFilter(StoreFileEditable.FIELD_SIZE, 1L))) {
//                transaction.remove(i.next());
//            }
//
//            obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(2);
//            transaction.save(obj);
//            newObj[0] = obj;
//        });
//
//        testFind(domainObjectSource, new HashFilter(StoreFileReadable.FIELD_SIZE, 2L), newObj[0].getId());
//    }

    private List<StoreFileReadable> initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        List<StoreFileReadable> result = new ArrayList<>();
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(i % 10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                transaction.save(obj);
                result.add(obj);
            }
        });
        return result;
    }
}
