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
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import com.infomaximum.database.domainobject.DomainDataTest;
import org.assertj.core.api.Assertions;
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

        new Schema.Builder().withDomain(StoreFileReadable.class).build(rocksDBProvider);
    }

    private com.infomaximum.database.schema.newschema.Schema ensureSchema(Class<? extends DomainObject>... domains) throws DatabaseException {
        com.infomaximum.database.schema.newschema.Schema schema = com.infomaximum.database.schema.newschema.Schema.read(rocksDBProvider);
        for (Class<? extends DomainObject> domain : domains) {
            StructEntity structEntity = Schema.getEntity(domain);
            schema.createTable(structEntity);
        }
        return schema;
    }

    @Test
    public void createAll() throws Exception {
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(ExchangeFolderReadable.class, StoreFileReadable.class);
        testNotWorking();
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        testWorking();
    }

    @Test
    public void createPartial() throws Exception {
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(ExchangeFolderReadable.class, StoreFileReadable.class);
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);

        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();
        rocksDBProvider.dropColumnFamily(entity.getColumnFamily());
        testNotWorking();

        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();
        testWorking();
    }

    @Test
    public void createIndexAndIndexingDataAfterDropIndex() throws Exception {
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(ExchangeFolderReadable.class, StoreFileReadable.class);
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);
        new DomainService(rocksDBProvider, schema)
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
                () -> rocksDBProvider.dropColumnFamily(entity.getIndexColumnFamily()),
                schema);

        checkIndexExist(entity,
                () -> entity.getHashIndexes().forEach(this::removeIndex),
                schema);

        checkIndexExist(entity,
                () -> entity.getPrefixIndexes().forEach(this::removeIndex),
                schema);

        checkIndexExist(entity,
                () -> entity.getIntervalIndexes().forEach(this::removeIndex),
                schema);

        checkIndexExist(entity,
                () -> entity.getRangeIndexes().forEach(this::removeIndex),
                schema);

        checkIndexExist(entity,
                () -> {
                    entity.getHashIndexes().forEach(this::removeIndex);
                    entity.getRangeIndexes().forEach(this::removeIndex);
                },
                schema);
    }

    @Test
    public void validateUnknownColumnFamily() throws Exception {
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(ExchangeFolderReadable.class, StoreFileReadable.class);
        createDomain(StoreFileReadable.class);

        rocksDBProvider.createColumnFamily("com.infomaximum.store.StoreFile.some_prefix");
        Assertions.assertThatExceptionOfType(InconsistentDatabaseException.class).isThrownBy(() -> new DomainService(rocksDBProvider, schema)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute());
    }

    @Test
    public void remove() throws Exception{
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(ExchangeFolderReadable.class, StoreFileReadable.class);
        createDomain(StoreFileReadable.class);
        Assertions.assertThat(schema.getDbSchema().getTables()).hasSize(2);

        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.REMOVAL)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.REMOVAL)
                .setDomain(Schema.getEntity(ExchangeFolderReadable.class))
                .execute();

        Assert.assertArrayEquals(new String[] {com.infomaximum.database.schema.newschema.Schema.SERVICE_COLUMN_FAMILY}, rocksDBProvider.getColumnFamilies());
        Assertions.assertThat(schema.getDbSchema().getTables()).isEmpty();
    }

    @Test
    public void removeAndValidate() throws Exception{
        com.infomaximum.database.schema.newschema.Schema schema = ensureSchema(StoreFileReadable.class);
        createDomain(StoreFileReadable.class);

        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileReadable.class))
                .execute();

        Assert.assertArrayEquals(new String[] {com.infomaximum.database.schema.newschema.Schema.SERVICE_COLUMN_FAMILY}, rocksDBProvider.getColumnFamilies());
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

    private void checkIndexExist(StructEntity entity, Producer before, com.infomaximum.database.schema.newschema.Schema schema) throws Exception {
        before.accept();
        new DomainService(rocksDBProvider, schema)
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
