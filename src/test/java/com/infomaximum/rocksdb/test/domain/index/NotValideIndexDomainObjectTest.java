package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by kris on 22.04.17.
 */
public class NotValideIndexDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(NotValideIndexDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);

        try {
            domainObjectSource.find(StoreFileReadable.class, new IndexFilter("zzzzz", null));
            Assert.fail();
        } catch (Exception ignore) {}


        try {
            domainObjectSource.find(StoreFileReadable.class, new IndexFilter("xxxxx", null).appendField("yyyyy", null));
            Assert.fail();
        } catch (Exception ignore) {}

        rocksDataBase.close();
    }
}
