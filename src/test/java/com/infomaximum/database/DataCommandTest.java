package com.infomaximum.database;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.exception.UnexpectedEndObjectException;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;


public class DataCommandTest extends StoreFileDataTest {

    //All iterator test
    @Test
    public void selectAllIterator() throws Exception {
        final int insertedRecordCount = 10;
        Collection<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource.select("StoreFile", "com.infomaximum.store")){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test()
    public void selectAllIteratorNoneObjects() throws Exception {
        try (RecordIterator iterator = recordSource.select("StoreFile", "com.infomaximum.store")){
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    public void checkInnerStructure() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }

        try (RecordIterator iterator = recordSource.select("StoreFile", "com.infomaximum.store")){
            Assertions.assertThatThrownBy(() -> {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }).isInstanceOf(UnexpectedEndObjectException.class);
        }
    }

    @Test
    public void orderingIterate() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource.select("StoreFile", "com.infomaximum.store")){
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                if (prevId == storeFile.getId()) Assertions.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assertions.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assertions.assertThat(insertedRecordCount).isEqualTo(iteratedRecordCount);
        }
    }
//
//    @Test //todo after insert
//    public void iterateTransactional() throws Exception {
//        try (com.infomaximum.database.domainobject.Transaction transaction = domainObjectSource.buildTransaction()) {
//            // insert
//            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
//            obj.setSize(10);
//            obj.setFormat(FormatType.B);
//            transaction.save(obj);
//
//            Assert.assertEquals(10L, transaction.get(StoreFileReadable.class, obj.getId()).getSize());
//
//            // change
//            obj.setSize(20);
//            transaction.save(obj);
//
//            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
//                Assert.assertEquals(20L, i.next().getSize());
//            }
//
//            // change
//            obj.setFormat(null);
//            transaction.save(obj);
//
//            StoreFileReadable storedObj = transaction.get(StoreFileReadable.class, obj.getId());
//            Assert.assertNull(storedObj.getFormat());
//
//            transaction.commit();
//        }
//    }
//
//    @Test
//    public void iterateAndChange() throws Exception {
//        final int insertedRecordCount = 10;
//        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);
//
//        try (Transaction transaction = domainObjectSource.buildTransaction()) {
//            try (IteratorEntity<StoreFileReadable> i = transaction.find(StoreFileReadable.class, EmptyFilter.INSTANCE)) {
//                while (i.hasNext()) {
//                    StoreFileReadable current = i.next();
//                    if (current.getId() == insertedRecordCount) {
//                        break;
//                    }
//
//                    StoreFileEditable newNext = transaction.get(StoreFileEditable.class, current.getId() + 1);
//                    newNext.setDouble(Double.POSITIVE_INFINITY);
//                    newNext.setFileName(UUID.randomUUID().toString());
//                    transaction.save(newNext);
//
//                    StoreFileReadable next = i.next();
//                    Assert.assertEquals("name", next.getFileName());
//                    Assert.assertNull(next.getDouble());
//                }
//            }
//        }
//    }
//
//    @Test
//    public void removeAndFind() throws Exception {
//        final int insertedRecordCount = 10;
//        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);
//
//        domainObjectSource.executeTransactional(transaction -> {
//            transaction.remove(transaction.get(StoreFileEditable.class, 1));
//            transaction.remove(transaction.get(StoreFileEditable.class, 5));
//
//            Assert.assertNull(transaction.get(StoreFileReadable.class, 1));
//            testFind(transaction, EmptyFilter.INSTANCE, 2,3,4,6,7,8,9,10);
//        });
//
//        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1));
//        testFind(domainObjectSource, EmptyFilter.INSTANCE, 2,3,4,6,7,8,9,10);
//    }

    //Hash iterator test
    @Test
    @DisplayName("Проверка HashIndex итератора. Находит ВСЕ StoreFiles объекты по заданному HashIndex для одного поля")
    public void selectHashIteratorForAll() throws Exception {
        final int insertedRecordCount = 10;
        Collection<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит только один StoreFiles объекты по заданному HashIndex для одного поля")
    public void selectHashIteratorOnlyOne() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> dbData = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 2L))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, dbData.get(2));
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит некоторые StoreFiles объекты по заданному HashIndex для двух полей")
    public void selectHashIteratorTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> dbData = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SIZE, 2L)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, dbData.get(2));
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Находит ВСЕ StoreFiles объекты по заданному HashIndex для двух полей")
    public void selectHashIteratorTwoFieldsAllMatch() throws Exception {
        final int insertedRecordCount = 10;
        List<? extends DomainObject> expected = initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, true)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            List<Record> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            assertContainsExactlyDomainObjects(actual, expected);
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Не находит объекты по заданному HashIndex")
    public void selectHashIteratorTwoFieldsNoneMatch() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, false)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }


    @Test
    @DisplayName("Проверка HashIndex итератора. Не должен упасть с NPE при отстутствии объектов в бд")
    public void selectHashIteratorNoneObjects() throws Exception {
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_SINGLE, false)
                        .appendField(StoreFileReadable.FIELD_FILE_NAME, "name"))) {
            Assertions.assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Не должен упасть с NPE при удалении ключа с id 2 [00 00 00 00 00 00 00 02]")
    public void checkInnerStructureHashIterator() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            transaction.delete(Schema.getEntity(StoreFileReadable.class).getColumnFamily(), TypeConvert.pack(2L));
            transaction.commit();
        }
        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))) {
            while (iterator.hasNext()) {
                iterator.next();
            }
        }
    }

    @Test
    @DisplayName("Проверка HashIndex итератора. Проверка порядка id")
    public void orderingIterateHashIndex() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        try (RecordIterator iterator = recordSource
                .select("StoreFile", "com.infomaximum.store", new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"))){
            int iteratedRecordCount = 0;
            long prevId = 0;
            while (iterator.hasNext()) {
                Record storeFile = iterator.next();

                if (prevId == storeFile.getId()) Assertions.fail("Fail next object");
                if (prevId >= storeFile.getId()) Assertions.fail("Fail sort id to iterators");
                prevId = storeFile.getId();
                ++iteratedRecordCount;
            }
            Assertions.assertThat(insertedRecordCount).isEqualTo(iteratedRecordCount);
        }
    }


    private List<? extends DomainObject> initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        List<StoreFileReadable> result = new ArrayList<>();
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(i);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
//                obj.setFormat(FormatType.B);
                transaction.save(obj);
                result.add(obj);
            }
        });
        return result;
    }
}
