package com.infomaximum.database.maintenance;

import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
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
    public void createIndexAndIndexingData() throws Exception {
        StructEntity entity = Schema.getEntity(StoreFileReadable.class);
        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 0; i < 100; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("Test");
                obj.setSize(i);
                transaction.save(obj);
            }
        });

        rocksDBProvider.dropColumnFamily("com.infomaximum.store.StoreFile.prefixindex.file_name:java.lang.String");
        rocksDBProvider.dropColumnFamily("com.infomaximum.store.StoreFile.index.size:java.lang.Long");

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(entity)
                .execute();

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new IndexFilter(StoreFileReadable.FIELD_SIZE, 10L))) {
            Assert.assertNotNull(iter.next());
        }

        try (IteratorEntity iter = domainObjectSource.find(StoreFileReadable.class, new PrefixIndexFilter(StoreFileReadable.FIELD_FILE_NAME,"tes"))) {
            Assert.assertNotNull(iter.next());
        }
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
}
