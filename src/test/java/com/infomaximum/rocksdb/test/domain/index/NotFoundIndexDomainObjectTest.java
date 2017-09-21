package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exeption.runtime.NotFoundIndexDatabaseException;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
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

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, "uuid", "");
            Assert.fail();
        } catch (NotFoundIndexDatabaseException ignore) {}

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, new HashMap<String, Object>(){{
                put(ExchangeFolderReadable.FIELD_UUID, "");
                put(ExchangeFolderReadable.FIELD_SYNC_DATE, "");
            }});
            Assert.fail();
        } catch (NotFoundIndexDatabaseException ignore) {}

        rocksDataBase.destroy();
    }

}
