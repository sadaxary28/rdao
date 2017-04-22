package com.infomaximum.rocksdb;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by kris on 22.04.17.
 */
public class RocksDataTest {

    protected Path pathDataBase;

    @Before
    public void init() throws IOException {
        pathDataBase = Files.createTempDirectory("rocksdb");
        pathDataBase.toAbsolutePath().toFile().deleteOnExit();
    }

    @After
    public void destroy() throws IOException {
        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
