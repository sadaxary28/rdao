package com.infomaximum.rocksdb;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import com.infomaximum.util.RandomUtil;
import org.junit.Test;

import java.util.UUID;

public class ReadTest  extends BaseTest {

    @Test
    public void iterateRecords() throws Exception {
        final int recordCount = 50 * 1000;

        domainObjectSource.createEntity(RecordReadable.class);

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordEditable rec = domainObjectSource.create(RecordEditable.class);
            rec.setString1("some value");
            domainObjectSource.save(rec, transaction);
        }
        transaction.commit();

        PerfomanceTest.test(100, step -> {
            try(IteratorEntity<RecordReadable> i = domainObjectSource.iterator(RecordReadable.class)) {
                while (i.hasNext()) {
                    RecordReadable rec = i.next();
                }
            }
        });
    }

    @Test
    public void findAllByStringIndexedRecords() throws Exception {
        final int recordCount = 100 * 1000;

        domainObjectSource.createEntity(RecordIndexReadable.class);

        final String fixedString = "some value";

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordIndexEditable rec = domainObjectSource.create(RecordIndexEditable.class);
            rec.setString1((i % 100) == 0 ? fixedString : UUID.randomUUID().toString());
            domainObjectSource.save(rec, transaction);
        }
        transaction.commit();

        PerfomanceTest.test(1, step -> {
            IteratorEntity<RecordIndexEditable> i = domainObjectSource.findAll(RecordIndexEditable.class, RecordIndexEditable.FIELD_STRING_1, fixedString);
            while (i.hasNext()) {
                RecordIndexEditable rec = i.next();
            }
        });
    }

    @Test
    public void findAllByLongIndexedRecords() throws Exception {
        final int recordCount = 100 * 1000;

        domainObjectSource.createEntity(RecordIndexReadable.class);

        final long fixedLong = 500;

        Transaction transaction = domainObjectSource.getEngineTransaction().createTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordIndexEditable rec = domainObjectSource.create(RecordIndexEditable.class);
            rec.setLong1((i % 100) == 0 ? fixedLong : RandomUtil.random.nextLong());
            domainObjectSource.save(rec, transaction);
        }
        transaction.commit();

        PerfomanceTest.test(1, step -> {
            IteratorEntity<RecordIndexEditable> i = domainObjectSource.findAll(RecordIndexEditable.class, RecordIndexEditable.FIELD_LONG_1, fixedLong);
            while (i.hasNext()) {
                RecordIndexEditable rec = i.next();
            }
        });
    }
}
