package com.infomaximum.rocksdb;

import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 23.08.16.
 */
public class EmptyTest extends RocksDataTestAssert {

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();


        Assert.assertTrue(true);

        rocksDataBase.destroy();
    }

}
