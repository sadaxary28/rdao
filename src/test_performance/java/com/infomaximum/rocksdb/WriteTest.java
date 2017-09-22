package com.infomaximum.rocksdb;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class WriteTest extends BaseTest {

    @Test
    public void createNonIndexedRecords1() throws Exception {
        domainObjectSource.createEntity(RecordReadable.class);

        PerfomanceTest.test(1000, step-> {
            Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
            RecordEditable rec = domainObjectSource.create(RecordEditable.class);
            rec.setString1("some value");
            domainObjectSource.save(rec, transaction);
            transaction.commit();
        });
    }

    @Test
    public void createNonIndexedRecords2() throws Exception {
        domainObjectSource.createEntity(RecordReadable.class);

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        PerfomanceTest.test(100000, step-> {
            RecordEditable rec = domainObjectSource.create(RecordEditable.class);
            rec.setString1("some value");
            domainObjectSource.save(rec, transaction);
        });
        transaction.commit();
    }
}
