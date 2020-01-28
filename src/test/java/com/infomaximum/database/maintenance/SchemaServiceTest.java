package com.infomaximum.database.maintenance;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.TableNotFoundException;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.domain.ExchangeFolderEditable;
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest { //todo V.Bukharkin дополнить тесты

    //todo add install
    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);

        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();

        Assert.assertTrue(true);
    }

//    @Test
//    public void validateInvalidScheme() throws DatabaseException {
//        try {
//            new SchemaService(rocksDBProvider)
//                    .setNamespace("com.infomaximum.store")
//                    .setValidationMode(true)
//                    .setSchema(Schema.read(rocksDBProvider))
//                    .execute();
//            Assert.fail();
//        } catch (InconsistentDatabaseException e) {
//            Assert.assertTrue(true);
//        }
//    }

    @Test
    public void removeInvalidScheme() throws DatabaseException {
        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.REMOVAL)
                .setValidationMode(false)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
        Assert.assertTrue(true);
    }

//    @Test
//    public void validateUnknownColumnFamily() throws Exception {
//        createDomain(ExchangeFolderEditable.class);
//        createDomain(StoreFileReadable.class);
//
//        rocksDBProvider.createColumnFamily("com.infomaximum.store.new_StoreFile.some_prefix");
//
//        try {
//            new SchemaService(rocksDBProvider)
//                    .setNamespace("com.infomaximum.store")
//                    .setValidationMode(true)
//                    .setSchema(Schema.read(rocksDBProvider))
//                    .execute();
//            Assert.fail();
//        } catch (InconsistentDatabaseException e) {
//            Assert.assertTrue(true);
//        }
//    }

    @Test
    public void validateWithIgnoringNotOwnedColumnFamily() throws Exception {
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);

        rocksDBProvider.createColumnFamily("com.new_infomaximum.new_StoreFile.some_prefix");

        new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setValidationMode(true)
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInaccurateData() throws Exception {
        try {
            createDomain(StoreFileEditable.class);
            Assert.fail();
        } catch (TableNotFoundException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateCoherentData() throws Exception {
        createDomain(ExchangeFolderEditable.class);
        createDomain(StoreFileEditable.class);

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
                .setSchema(Schema.read(rocksDBProvider))
                .execute();
    }
}
