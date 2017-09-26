package com.infomaximum.rocksdb.test;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataBase;
import org.junit.Test;

/**
 * Created by kris on 23.08.16.
 */
public class EmptyTest extends RocksDataTest {

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        rocksDataBase.close();
    }

}
