package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.ExchangeFolder;
import com.infomaximum.rocksdb.exception.NotFoundIndexException;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by kris on 22.04.17.
 */
public class NotFoundIndexDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(NotFoundIndexDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        try {
            domainObjectSource.find(ExchangeFolder.class, "uuid", "");
            Assert.fail();
        } catch (NotFoundIndexException ignore) {}

        try {
            domainObjectSource.find(ExchangeFolder.class, new HashMap<String, Object>(){{
                put("uuid", "");
                put("syncDate", "");
            }});
            Assert.fail();
        } catch (NotFoundIndexException ignore) {}

        rocksDataBase.destroy();
    }

}
