package com.infomaximum.database.domainobject.index;

import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.exception.runtime.IndexNotFoundException;
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.database.domainobject.ExchangeFolderDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class NotFoundIndexDomainObjectTest extends ExchangeFolderDataTest {

    @Test
    public void run() throws Exception {
        try {
            domainObjectSource.find(ExchangeFolderReadable.class, new HashFilter("uuid", ""));
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}

        try {
            domainObjectSource.find(ExchangeFolderReadable.class, new HashFilter(ExchangeFolderReadable.FIELD_UUID, "")
                .appendField(ExchangeFolderReadable.FIELD_SYNC_DATE, ""));
            Assert.fail();
        } catch (IndexNotFoundException ignore) {}
    }
}
