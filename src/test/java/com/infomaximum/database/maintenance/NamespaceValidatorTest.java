package com.infomaximum.database.maintenance;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.InconsistentDatabaseException;
import com.infomaximum.database.domainobject.DomainDataTest;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceValidatorTest extends DomainDataTest {

    @Test
    public void validateEmptySchema() throws DatabaseException {
        new NamespaceValidator(rocksDBProvider).execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateValidSchema() throws DatabaseException {

        rocksDBProvider.createColumnFamily("com.infomaximum.database.exception");
        rocksDBProvider.createColumnFamily("com.infomaximum.database.maintenance");

        rocksDBProvider.createColumnFamily("com.infomaximum.rocksdb.exception");
        rocksDBProvider.createColumnFamily("com.infomaximum.rocksdb.maintenance");

        new NamespaceValidator(rocksDBProvider)
                .withNamespace("com.infomaximum.database")
                .withNamespace("com.infomaximum.rocksdb")
                .execute();
        Assert.assertTrue(true);
    }

    @Test
    public void validateInvalidSchema() throws DatabaseException {

        rocksDBProvider.createColumnFamily("com.infomaximum.database.exception");
        rocksDBProvider.createColumnFamily("com.infomaximum.database.maintenance");

        rocksDBProvider.createColumnFamily("com.infomaximum.rocksdb.exception");
        rocksDBProvider.createColumnFamily("com.infomaximum.rocksdb.maintenance");

        rocksDBProvider.createColumnFamily("com.infomaximum.maintenance");

        try {
            new NamespaceValidator(rocksDBProvider)
                    .withNamespace("com.infomaximum.database")
                    .withNamespace("com.infomaximum.rocksdb")
                    .execute();
            Assert.fail();
        } catch (InconsistentDatabaseException e) {
            Assert.assertTrue(true);
        }
    }
}
