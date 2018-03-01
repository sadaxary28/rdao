package com.infomaximum.database.utils;

import com.infomaximum.database.exception.DatabaseException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;

public class PathUtilsTest {

    @Test
    public void checkPath() throws Exception {
        PathUtils.checkPath(Paths.get("c:/test"));

        try {
            PathUtils.checkPath(Paths.get("test"));
            Assert.fail();
        } catch (DatabaseException ignored) {
        }

        try {
            PathUtils.checkPath(Paths.get("c:/привет"));
            Assert.fail();
        } catch (DatabaseException ignored) {
        }
    }
}
