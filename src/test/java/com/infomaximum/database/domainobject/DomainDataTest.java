package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.FieldValueNotFoundException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.database.schema.Field;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Set;

public abstract class DomainDataTest extends RocksDataTest {

    protected RocksDBProvider rocksDBProvider;

    protected DomainObjectSource domainObjectSource;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);
        Schema.create(rocksDBProvider);
    }

    @After
    public void destroy() throws Exception {
        if (rocksDBProvider != null) {
            rocksDBProvider.close();
        }

        super.destroy();
    }

    protected void createDomain(Class<? extends DomainObject> clazz) throws DatabaseException {
        createDomain(clazz, rocksDBProvider);
    }

    protected void createDomain(Class<? extends DomainObject> clazz, RocksDBProvider rocksDBProvider) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(clazz))
                .execute();
    }

    protected static void checkLoadedState(DomainObject target, Set<Integer> loadingFields) {
        for (Integer field : loadingFields) {
            target.get(field);
        }

        for (Field field : target.getStructEntity().getFields()) {
            if (loadingFields.contains(field.getNumber())) {
                continue;
            }
            try {
                target.get(field.getNumber());
                Assert.fail();
            } catch (FieldValueNotFoundException e) {
                Assert.assertTrue(true);
            }
        }
    }
}
