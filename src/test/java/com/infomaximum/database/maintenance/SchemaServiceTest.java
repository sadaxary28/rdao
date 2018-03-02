package com.infomaximum.database.maintenance;

import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.exception.InconsistentDatabaseException;
import com.infomaximum.domain.ExchangeFolderEditable;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(StoreFileReadable.class);

        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();

        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidScheme() throws DatabaseException {
        try {
            new SchemaService(rocksDBProvider)
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
        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(false)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new SchemaService(rocksDBProvider)
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

        rocksDBProvider.createColumnFamily("com.infomaximum.store.new_StoreFile.some_prefix");

        try {
            new SchemaService(rocksDBProvider)
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

        rocksDBProvider.createColumnFamily("com.new_infomaximum.new_StoreFile.some_prefix");

        new SchemaService(rocksDBProvider)
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

        rocksDBProvider.createColumnFamily(ignoringNamespace + ".temp1");
        rocksDBProvider.createColumnFamily(ignoringNamespace + ".temp2");

        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .appendIgnoringNamespace(ignoringNamespace)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);

        rocksDBProvider.createColumnFamily("com.infomaximum.store.notignore.temp2");
        try {
            new SchemaService(rocksDBProvider)
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
            new SchemaService(rocksDBProvider)
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

        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(new Schema.Builder()
                        .withDomain(StoreFileEditable.class)
                        .build())
                .execute();
    }
}
