package com.infomaximum.rocksdb.maintenance;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.exception.InconsistentDatabaseException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.SchemaService;
import com.infomaximum.rocksdb.domain.ExchangeFolderEditable;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(StoreFileReadable.class);

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();

        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidScheme() throws DatabaseException {
        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum.store")
                    .setValidationMode(true)
                    .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void removeInvalidScheme() throws DatabaseException {
        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(false)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateUnknownColumnFamily() throws Exception {
        createDomain(StoreFileReadable.class);

        rocksDataBase.createColumnFamily("com.infomaximum.store.new_StoreFile.some_prefix");

        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum.store")
                    .setValidationMode(true)
                    .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateWithIgnoringNotOwnedColumnFamily() throws Exception {
        createDomain(StoreFileReadable.class);

        rocksDataBase.createColumnFamily("com.new_infomaximum.new_StoreFile.some_prefix");

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateWithIgnoringNamespaces() throws Exception {
        final String ignoringNamespace = "com.infomaximum.store.ignore";

        createDomain(StoreFileReadable.class);

        rocksDataBase.createColumnFamily(ignoringNamespace + ".temp1");
        rocksDataBase.createColumnFamily(ignoringNamespace + ".temp2");

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .appendIgnoringNamespace(ignoringNamespace)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);

        rocksDataBase.createColumnFamily("com.infomaximum.store.notignore.temp2");
        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum.store")
                    .setValidationMode(true)
                    .appendIgnoringNamespace(ignoringNamespace)
                    .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateInaccurateData() throws Exception {
        createDomain(StoreFileEditable.class);
        createDomain(ExchangeFolderEditable.class);

        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFolderId(256);
            transaction.save(obj);
        });

        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum.store")
                    .setValidationMode(true)
                    .setSchema(new Schema.Builder()
                            .withDomain(StoreFileEditable.class)
                            .build())
                    .execute();
            Assert.fail();
        } catch (ForeignDependencyException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateCoherentData() throws Exception {
        createDomain(StoreFileEditable.class);
        createDomain(ExchangeFolderEditable.class);

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFolderId(folder.getId());
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            transaction.save(obj);
        });

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(new Schema.Builder()
                        .withDomain(StoreFileEditable.class)
                        .build())
                .execute();
    }
}
