package com.infomaximum.database.utils;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.nio.file.Paths;

public class PathUtilsTest {

    @Test
    public void checkPath() throws Exception {
        PathUtils.checkPath(Paths.get("c:/test"));

        try {
            PathUtils.checkPath(Paths.get("test"));
            Assert.fail();
        } catch (RocksDBException ignored) {
        }

        try {
            PathUtils.checkPath(Paths.get("c:/привет"));
            Assert.fail();
        } catch (RocksDBException ignored) {
        }
    }
}
