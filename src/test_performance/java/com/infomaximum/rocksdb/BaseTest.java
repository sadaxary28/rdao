package com.infomaximum.rocksdb;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class BaseTest {

    private Path pathDataBase;
    private RocksDataBase rocksDataBase;
    protected DomainObjectSource domainObjectSource;

    @Before
    public void init() throws Exception {
        pathDataBase = Files.createTempDirectory("rocksdb");
        pathDataBase.toAbsolutePath().toFile().deleteOnExit();

         rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

         domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
    }

    @After
    public void destroy() throws IOException {
        rocksDataBase.close();
        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
