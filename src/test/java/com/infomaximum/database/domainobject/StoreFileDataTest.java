package com.infomaximum.database.domainobject;

import com.infomaximum.domain.StoreFileReadable;
import org.junit.Before;

public abstract class StoreFileDataTest extends DomainDataTest {

    @Before
    public void init() throws Exception {
        super.init();

        createDomain(StoreFileReadable.class);
    }
}
