package com.infomaximum.database.domainobject;

import com.infomaximum.domain.ExchangeFolderReadable;
import org.junit.Before;

public abstract class ExchangeFolderDataTest extends DomainDataTest {

    @Before
    public void init() throws Exception {
        super.init();

        createDomain(ExchangeFolderReadable.class);
    }
}
