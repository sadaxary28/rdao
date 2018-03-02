package com.infomaximum.database.domainobject;

import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.After;
import org.junit.Before;

public abstract class DomainDataTest extends RocksDataTest {

    protected RocksDBProvider rocksDBProvider;

    protected DomainObjectSource domainObjectSource;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);
    }

    @After
    public void destroy() throws Exception {
        rocksDBProvider.close();

        super.destroy();
    }

    protected void createDomain(Class<? extends DomainObject> clazz) throws DatabaseException {
        new Schema.Builder().withDomain(clazz).build();
        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(clazz))
                .execute();
    }
}
