package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


public abstract class DomainDataJ5Test extends RocksDataTest {

    protected RocksDBProvider rocksDBProvider;

    protected DomainObjectSource domainObjectSource;

    @BeforeEach
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);
    }

    @AfterEach
    public void destroy() throws Exception {
        if (rocksDBProvider != null) {
            rocksDBProvider.close();
        }

        super.destroy();
    }

    protected void createDomain(Class<? extends DomainObject> clazz) throws DatabaseException {
        createDomain(clazz, rocksDBProvider);
    }

    protected void createDomain(Class<? extends DomainObject> clazz, RocksDBProvider rocksDBProvider) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        new DomainService(rocksDBProvider, schema)
                .setChangeMode(ChangeMode.CREATION)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(clazz))
                .execute();
    }
}
