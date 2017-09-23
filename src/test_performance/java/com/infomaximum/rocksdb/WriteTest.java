package com.infomaximum.rocksdb;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import com.infomaximum.util.RandomUtil;
import org.junit.Test;

import java.util.UUID;

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

    @Test
    public void createIndexedRecords() throws Exception {
        domainObjectSource.createEntity(RecordIndexReadable.class);

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        PerfomanceTest.test(100000, step-> {
            RecordIndexEditable rec = domainObjectSource.create(RecordIndexEditable.class);
            rec.setString1(UUID.randomUUID().toString());
            rec.setLong1(RandomUtil.random.nextLong());
            domainObjectSource.save(rec, transaction);
        });
        transaction.commit();
    }
}
