package com.infomaximum.rocksdb.test;

import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import org.junit.After;
import org.junit.Before;

public abstract class StoreFileDataTest extends RocksDataTest {

    private RocksDataBase rocksDataBase;

    protected DataSource dataSource;
    protected DomainObjectSource domainObjectSource;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        dataSource = new RocksDBDataSourceImpl(rocksDataBase);
        domainObjectSource = new DomainObjectSource(dataSource);
        domainObjectSource.createEntity(StoreFileReadable.class);
    }

    @After
    public void destroy() throws Exception {
        rocksDataBase.close();

        super.destroy();
    }
}
