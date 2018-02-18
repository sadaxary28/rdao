package com.infomaximum.rocksdb;

import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.rocksdb.test.DomainDataTest;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class WriteTest extends DomainDataTest {

    @Test
    public void createNonIndexedRecords1() throws Exception {
        createDomain(RecordReadable.class);

        PerfomanceTest.test(1000000, step-> {
            Transaction transaction = domainObjectSource.buildTransaction();
            RecordEditable rec = transaction.create(RecordEditable.class);
            rec.setString1("some value");
            transaction.save(rec);
            transaction.commit();
        });
    }

    @Test
    public void createNonIndexedRecords2() throws Exception {
        createDomain(RecordReadable.class);

        Transaction transaction = domainObjectSource.buildTransaction();
        PerfomanceTest.test(1000000, step-> {
            RecordEditable rec = transaction.create(RecordEditable.class);
            rec.setString1("some value");
            transaction.save(rec);
        });
        transaction.commit();
    }

    @Test
    public void createIndexedRecords() throws Exception {
        createDomain(RecordIndexReadable.class);

        long counter[] = new long[] {0};

        Transaction transaction = domainObjectSource.buildTransaction();
        PerfomanceTest.test(500000, step-> {
            RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
            long val = ++counter[0];
            rec.setString1(Long.toString(val));
            rec.setLong1(val);
            transaction.save(rec);
        });
        transaction.commit();
    }
}
