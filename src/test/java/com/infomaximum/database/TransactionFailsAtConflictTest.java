package com.infomaximum.database;

import com.infomaximum.database.domainobject.DomainDataTest;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.domain.DataTestEditable;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import org.junit.Test;

public class TransactionFailsAtConflictTest extends DomainDataTest {

    @Test
    public void test() throws Exception {
        final int fillCount = 761_971;
        final int deleteCount = 1;

        fillData(fillCount);

        rocksDBProvider.close();
        rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        domainObjectSource = new DomainObjectSource(rocksDBProvider);

        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 1; i <= deleteCount; i++) {
                DataTestEditable dataTestEditable = transaction.get(DataTestEditable.class, i);
                transaction.remove(dataTestEditable);
            }
        });
    }

    private void fillData(int count) throws Exception {
        createDomain(DataTestEditable.class);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < count; ++i) {
                DataTestEditable generalEditable = transaction.create(DataTestEditable.class);
                generalEditable.setValue("value:" + i);
                transaction.save(generalEditable);
            }
        });
    }
}