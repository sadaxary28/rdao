package com.infomaximum.rocksdb.test;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDBDataSource;
import org.junit.After;
import org.junit.Before;

public abstract class DomainDataTest extends RocksDataTest {

    protected RocksDataBase rocksDataBase;

    protected DataSource dataSource;
    protected DomainObjectSource domainObjectSource;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        dataSource = new RocksDBDataSource(rocksDataBase);
        domainObjectSource = new DomainObjectSource(dataSource);
    }

    @After
    public void destroy() throws Exception {
        rocksDataBase.close();

        super.destroy();
    }

    protected void createDomain(Class<? extends DomainObject> clazz) throws DatabaseException {
        new Schema.Builder().withDomain(clazz).build();
        new DomainService(dataSource)
                .setCreationMode(true)
                .execute(Schema.getEntity(clazz));
    }
}
