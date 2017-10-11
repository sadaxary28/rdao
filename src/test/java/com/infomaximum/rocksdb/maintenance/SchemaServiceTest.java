package com.infomaximum.rocksdb.maintenance;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;
import com.infomaximum.database.maintenance.SchemaService;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateValidScheme() throws DatabaseException {
        createDomain(StoreFileReadable.class);

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(false)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();

        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidScheme() throws DatabaseException {
        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum")
                    .setCreationMode(false)
                    .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void createAndValidateScheme() throws DatabaseException {
        new SchemaService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(true)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateUnknownColumnFamily() throws Exception {
        new DomainService(dataSource).setCreationMode(true).execute(Schema.getEntity(StoreFileReadable.class));

        rocksDataBase.createColumnFamily("com.infomaximum.new_StoreFile.some_prefix");

        try {
            new SchemaService(dataSource)
                    .setNamespace("com.infomaximum")
                    .setCreationMode(false)
                    .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void validateWithIgnoringNotOwnedColumnFamily() throws Exception {
        new DomainService(dataSource).setCreationMode(true).execute(Schema.getEntity(StoreFileReadable.class));

        rocksDataBase.createColumnFamily("com.new_infomaximum.new_StoreFile.some_prefix");

        new SchemaService(dataSource)
                .setNamespace("com.infomaximum")
                .setCreationMode(false)
                .setSchema(new Schema.Builder().withDomain(StoreFileReadable.class).build())
                .execute();
        Assert.assertTrue(true);
    }
}
