package com.infomaximum.database.domainobject;

import com.infomaximum.database.RecordSource;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.FieldValueNotFoundException;
import com.infomaximum.database.schema.Field;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.Set;

public abstract class DomainDataTest extends RocksDataTest {

    protected RocksDBProvider rocksDBProvider;

    protected DomainObjectSource domainObjectSource;
    protected RecordSource recordSource;

    @BeforeEach
    public void init() throws Exception {
        super.init();

        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        Schema.create(rocksDBProvider);
        domainObjectSource = new DomainObjectSource(rocksDBProvider, true);
        recordSource = new RecordSource(rocksDBProvider);
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
        schema.createTable(Schema.resolve(clazz));
    }

    protected static void checkLoadedState(DomainObject target, Set<Integer> loadingFields) {
        for (Integer field : loadingFields) {
            target.get(field);
        }

        for (Field field : target.getStructEntity().getFields()) {
            if (loadingFields.contains(field.getNumber())) {
                continue;
            }
            Assertions.assertThrows(FieldValueNotFoundException.class, () -> target.get(field.getNumber()));
        }
    }
}
