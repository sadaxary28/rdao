package com.infomaximum.rocksdb;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class ReadTest  extends BaseTest {

    @Test
    public void iterateRecords1() throws Exception {
        final int recordCount = 1000 * 1000;

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordEditable rec = domainObjectSource.create(RecordEditable.class);
            rec.setString1("some value");
            domainObjectSource.save(rec, transaction);
        }
        transaction.commit();

        PerfomanceTest.test(1, step -> {
            IteratorEntity<RecordReadable> i = domainObjectSource.iterator(RecordReadable.class);
            while (i.hasNext()) {
                RecordReadable rec = i.next();
            }
        });
    }
}
