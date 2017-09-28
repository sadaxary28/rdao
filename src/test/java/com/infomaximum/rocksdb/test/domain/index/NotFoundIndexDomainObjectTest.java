package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.runtime.NotFoundIndexDatabaseException;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
import com.infomaximum.rocksdb.RocksDataBase;
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
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(ExchangeFolderReadable.class);

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, null, new HashMap<String, Object>() {{ put("uuid", "");}});
            Assert.fail();
        } catch (NotFoundIndexDatabaseException ignore) {}

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, null, new HashMap<String, Object>(){{
                put(ExchangeFolderReadable.FIELD_UUID, "");
                put(ExchangeFolderReadable.FIELD_SYNC_DATE, "");
            }});
            Assert.fail();
        } catch (NotFoundIndexDatabaseException ignore) {}

        rocksDataBase.close();
    }

}
