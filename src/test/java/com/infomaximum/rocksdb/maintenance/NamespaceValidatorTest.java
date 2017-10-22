package com.infomaximum.rocksdb.maintenance;

import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;
import com.infomaximum.database.maintenance.NamespaceValidator;
import com.infomaximum.rocksdb.test.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceValidatorTest extends DomainDataTest {

    @Test
    public void validateEmptySchema() throws DatabaseException {
        new NamespaceValidator(dataSource).execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateValidSchema() throws DatabaseException {

        dataSource.createColumnFamily("com.infomaximum.database.exeption");
        dataSource.createColumnFamily("com.infomaximum.database.maintenance");

        dataSource.createColumnFamily("com.infomaximum.rocksdb.exeption");
        dataSource.createColumnFamily("com.infomaximum.rocksdb.maintenance");

        new NamespaceValidator(dataSource)
                .withNamespace("com.infomaximum.database")
                .withNamespace("com.infomaximum.rocksdb")
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidSchema() throws DatabaseException {

        dataSource.createColumnFamily("com.infomaximum.database.exeption");
        dataSource.createColumnFamily("com.infomaximum.database.maintenance");

        dataSource.createColumnFamily("com.infomaximum.rocksdb.exeption");
        dataSource.createColumnFamily("com.infomaximum.rocksdb.maintenance");

        dataSource.createColumnFamily("com.infomaximum.maintenance");

        try {
            new NamespaceValidator(dataSource)
                    .withNamespace("com.infomaximum.database")
                    .withNamespace("com.infomaximum.rocksdb")
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }
}
