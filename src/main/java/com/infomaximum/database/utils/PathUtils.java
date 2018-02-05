package com.infomaximum.database.utils;

import com.google.common.base.CharMatcher;
import org.rocksdb.RocksDBException;

import java.nio.file.Path;

public class PathUtils {

    public static void checkPath(Path path) throws RocksDBException {
        if (!path.isAbsolute()) {
            throw new RocksDBException("RocksDB-paths is not absolute.");
        }

        if (!CharMatcher.ascii().matchesAllOf(path.toString())) {
            throw new RocksDBException("RocksDB-paths is not ascii-string.");
        }
    }
}
