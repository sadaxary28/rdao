package com.infomaximum.rocksdb;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;

import java.util.Iterator;

public class ReadTest  extends BaseTest {

    @Test
    public void iterateRecords1() throws Exception {
        final int recordCount = 10000;

        domainObjectSource.createEntity(RecordReadable.class);

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordEditable rec = domainObjectSource.create(RecordEditable.class);
            rec.setString1("some value");
            domainObjectSource.save(rec, transaction);
        }
        transaction.commit();

        PerfomanceTest.test(1, step -> {
            Iterator<RecordReadable> i = domainObjectSource.iterator(RecordReadable.class);
            while (i.hasNext()) {
                RecordReadable rec = i.next();
            }
        });
    }
}
