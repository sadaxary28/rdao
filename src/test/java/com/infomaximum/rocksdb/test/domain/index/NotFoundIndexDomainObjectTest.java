package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.database.exeption.runtime.NotFoundIndexException;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
import com.infomaximum.rocksdb.test.ExchangeFolderDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class NotFoundIndexDomainObjectTest extends ExchangeFolderDataTest {

    @Test
    public void run() throws Exception {
        try {
            domainObjectSource.find(ExchangeFolderReadable.class, new IndexFilter("uuid", ""));
            Assert.fail();
        } catch (NotFoundIndexException ignore) {}

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, new IndexFilter(ExchangeFolderReadable.FIELD_UUID, "")
                .appendField(ExchangeFolderReadable.FIELD_SYNC_DATE, ""));
            Assert.fail();
        } catch (NotFoundIndexException ignore) {}
    }
}
