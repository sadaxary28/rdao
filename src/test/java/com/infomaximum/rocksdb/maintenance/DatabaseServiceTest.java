package com.infomaximum.rocksdb.maintenance;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;
import com.infomaximum.database.maintenance.DatabaseService;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseServiceTest extends DomainDataTest {

    @Test
    public void validateEmptyScheme() throws DatabaseException {
        new DatabaseService(dataSource)
                .setCreationMode(true)
                .setNamespace("com.infomaximum")
                .execute();

        Assert.assertTrue(true);
    }

    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(StoreFileReadable.class);

        new DatabaseService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(false)
                .withDomain(StoreFileReadable.class)
                .execute();

        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidScheme() throws DatabaseException {
        try {
            new DatabaseService(dataSource)
                    .setNamespace("com.infomaximum")
                    .setCreationMode(false)
                    .withDomain(StoreFileReadable.class)
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new DatabaseService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(true)
                .withDomain(StoreFileReadable.class)
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateUnknownColumnFamily() throws Exception {
        new DomainService(dataSource).setCreationMode(true).execute(StructEntity.getInstance(StoreFileReadable.class));

        rocksDataBase.createColumnFamily("com.infomaximum.new_StoreFile.some_prefix");

        try {
            new DatabaseService(dataSource)
                    .setNamespace("com.infomaximum")
                    .setCreationMode(false)
                    .withDomain(StoreFileReadable.class)
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateWithIgnoringNotOwnedColumnFamily() throws Exception {
        new DomainService(dataSource).setCreationMode(true).execute(StructEntity.getInstance(StoreFileReadable.class));

        rocksDataBase.createColumnFamily("com.new_infomaximum.new_StoreFile.some_prefix");

        new DatabaseService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(false)
                .withDomain(StoreFileReadable.class)
                .execute();
        Assert.assertTrue(true);
    }
}
