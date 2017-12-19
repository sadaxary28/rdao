package com.infomaximum.rocksdb;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class RocksDBLoadOptionsTest extends RocksDataTest {

    private Path optionsFilePath;

    @Before
    @Override
    public void init() throws Exception {
        super.init();

        optionsFilePath = Paths.get(pathDataBase.toString() + ".ini");
        optionsFilePath.toFile().deleteOnExit();
    }

    @After
    @Override
    public void destroy() throws Exception {
        optionsFilePath.toFile().delete();

        super.destroy();
    }

    @Test
    public void fromFile() throws Exception {
        List<String> content = Arrays.asList(
                "[Version]",
                "rocksdb_version=5.7.3",
                "options_file_version=1.0",
                "[DBOptions]",
                "max_open_files=5",
                "max_total_wal_size=104857600",
                "info_log_level=WARN_LEVEL",
                "[CFOptions \"default\"]"
        );
        Files.write(optionsFilePath, content, StandardCharsets.UTF_8);

        try (Options options = RocksDB.loadOptionsFromFile(optionsFilePath.toString(), false)) {
            Assert.assertEquals(options.maxOpenFiles(), 5);
            Assert.assertEquals(options.maxTotalWalSize(), 104857600);
            Assert.assertEquals(options.infoLogLevel(), InfoLogLevel.WARN_LEVEL);
        }
    }

    @Test
    public void fromFileWithWrongOptions() throws Exception {
        List<String> content = Arrays.asList(
                "[Version]",
                "rocksdb_version=5.7.3",
                "options_file_version=1.0",
                "[DBOptions]",
                "max_open_files=5",
                "_unknown_=0",
                "info_log_level=WARN_LEVEL",
                "[CFOptions \"default\"]"
        );
        Files.write(optionsFilePath, content, StandardCharsets.UTF_8);

        try (Options options = RocksDB.loadOptionsFromFile(optionsFilePath.toString(), false)) {
            Assert.assertEquals(options.maxOpenFiles(), 5);
            Assert.assertEquals(options.infoLogLevel(), InfoLogLevel.WARN_LEVEL);
        } catch (RocksDBException e) {
            Assert.assertTrue(true);
            return;
        }

        Assert.fail();
    }

    @Test
    public void fromFileWithIgnoreWrongOptions() throws Exception {
        List<String> content = Arrays.asList(
                "[Version]",
                "rocksdb_version=5.7.3",
                "options_file_version=1.0",
                "[DBOptions]",
                "max_open_files=5",
                "_unknown_=0",
                "info_log_level=WARN_LEVEL",
                "[CFOptions \"default\"]"
        );
        Files.write(optionsFilePath, content, StandardCharsets.UTF_8);

        try (Options options = RocksDB.loadOptionsFromFile(optionsFilePath.toString(), true)) {
            Assert.assertEquals(options.maxOpenFiles(), 5);
            Assert.assertEquals(options.infoLogLevel(), InfoLogLevel.WARN_LEVEL);
        }
    }
}
