package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class NotValideIndexDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(NotValideIndexDomainObjectTest.class);

    @Test
    public void run() throws Exception {
//        RocksDataBase rocksDataBase = new RocksdbBuilder()
//                .withPath(pathDataBase)
//                .build();
//
//        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));
//
//        try {
//            domainObjectSource.find(StoreFile.class, "zzzzz", null);
//            Assert.fail();
//        } catch (Exception ignore) {}
//
//
//        try {
//            domainObjectSource.find(StoreFile.class, new HashMap<String, Object>(){{
//                put("xxxxx", null);
//                put("yyyyy", null);
//            }});
//            Assert.fail();
//        } catch (Exception ignore) {}
//
//        rocksDataBase.destroy();
    }

}
