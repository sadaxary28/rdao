package com.infomaximum.rocksdb.test.domain;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.StoreFile;
import com.infomaximum.rocksdb.struct.RocksDataBase;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class DomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(DomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));
        StoreFile storeFile = domainObjectSource.create(StoreFile.class);
        storeFile.setContentType("application/json");
        storeFile.setFileName("info.json");
        storeFile.setSize(1000L);
        storeFile.save();

        StoreFile storeFile1 = domainObjectSource.get(StoreFile.class, 1L);


        rocksDataBase.destroy();
    }

}
