package com.infomaximum.database.maintenance;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.filter.PrefixFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.InconsistentDatabaseException;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import com.infomaximum.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class DomainServiceTest extends DomainDataTest {

    @FunctionalInterface
    private interface Producer {

        void accept() throws DatabaseException;
    }

    @Before
    public void init() throws Exception {
        super.init();

        new Schema.Builder().withDomain(StoreFileReadable.class).build();
    }

    @Test
    public void createAll() throws Exception {
        testNotWorking();

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        testWorking();
    }

    @Test
    public void createPartial() throws Exception {
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();
        rocksDBProvider.dropColumnFamily(entity.getColumnFamily());
        testNotWorking();

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();
        testWorking();
    }

    @Test
    public void createIndexAndIndexingDataAfterDropIndex() throws Exception {
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);
        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i < 100; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("Test");
                obj.setSize(i);
                obj.setBegin(i);
                obj.setEnd(i+10);
                transaction.save(obj);
            }
        });

        checkIndexExist(entity,
                () -> rocksDBProvider.dropColumnFamily(entity.getIndexColumnFamily()));

        checkIndexExist(entity,
                () -> entity.getHashIndexes().forEach(this::removeIndex));

        checkIndexExist(entity,
                () -> entity.getPrefixIndexes().forEach(this::removeIndex));

        checkIndexExist(entity,
                () -> entity.getIntervalIndexes().forEach(this::removeIndex));

        checkIndexExist(entity,
                () -> entity.getRangeIndexes().forEach(this::removeIndex));

        checkIndexExist(entity,
                () -> {
                    entity.getHashIndexes().forEach(this::removeIndex);
                    entity.getRangeIndexes().forEach(this::removeIndex);
                });
    }

    @Test
    public void validateUnknownColumnFamily() throws Exception {
        createDomain(StoreFileReadable.class);

        rocksDBProvider.createColumnFamily("com.infomaximum.store.StoreFile.some_prefix");

        try {
            new DomainService(rocksDBProvider).setValidationMode(true).setDomain(Schema.getEntity(StoreFileReadable.class)).execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void remove() throws Exception{
        createDomain(StoreFileReadable.class);

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.REMOVAL)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        Assert.assertArrayEquals(new String[0], rocksDBProvider.getColumnFamilies());
    }

    @Test
    public void removeAndValidate() throws Exception{
        createDomain(StoreFileReadable.class);

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        Assert.assertArrayEquals(new String[0], rocksDBProvider.getColumnFamilies());
    }

    private void testNotWorking() throws Exception {
        try {
            testWorking();
            Assert.fail();
        } catch (DatabaseException ignoring) {
            Assert.assertTrue(true);
        }
    }

    private void testWorking() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Test");
            obj.setSize(100);
            obj.setFormat(FormatType.B);
            obj.setSingle(false);
            obj.setContentType("content");
            transaction.save(obj);
        });
    }

    private void removeIndex(BaseIndex index) {
        try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
            try (DBIterator it = domainObjectSource.createIterator(index.columnFamily)) {
                for (KeyValue kv = it.seek(new KeyPattern(index.attendant)); kv != null ; kv = it.next()) {
                    transaction.delete(index.columnFamily, kv.getKey());
                }
            }
            transaction.commit();
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkIndexExist(StructEntity entity, Producer before) throws Exception {
        before.accept();
        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_SIZE, 10L))) {
            Assert.assertNotNull(iter.next());
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new PrefixFilter(StoreFileReadable.FIELD_FILE_NAME,"tes"))) {
            Assert.assertNotNull(iter.next());
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new IntervalFilter(StoreFileReadable.FIELD_SIZE, 35L, 50L))) {
            long expectedId = 35;
            while (iter.hasNext()) {
                DomainObject st = iter.next();
                Assert.assertEquals(expectedId, st.getId());
                expectedId++;
            }
            Assert.assertEquals(50L, expectedId-1);
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class,
                new RangeFilter(new RangeFilter.IndexedField(StoreFileReadable.FIELD_BEGIN, StoreFileReadable.FIELD_END), 35L, 50L))) {
            long expectedId = 35-9;
            while (iter.hasNext()) {
                DomainObject st = iter.next();
                Assert.assertEquals(expectedId, st.getId());
                expectedId++;
            }
            Assert.assertEquals(50L, expectedId);
        }
    }
}
