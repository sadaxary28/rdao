package com.infomaximum.rocksdb.test;

import com.infomaximum.rocksdb.domain.StoreFileReadable;
import org.junit.Before;

public abstract class StoreFileDataTest extends DomainDataTest {

    @Before
    public void init() throws Exception {
        super.init();

        createDomain(StoreFileReadable.class);
    }
}
