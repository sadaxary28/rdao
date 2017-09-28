package com.infomaximum.rocksdb;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import com.infomaximum.util.RandomUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ReadTest  extends BaseTest {

    @Test
    public void iterateRecords() throws Exception {
        final int recordCount = 50 * 1000;

        domainObjectSource.createEntity(RecordReadable.class);

        Transaction transaction = domainObjectSource.buildTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordEditable rec = transaction.create(RecordEditable.class);
            rec.setString1("some value");
            transaction.save(rec);
        }
        transaction.commit();

        PerfomanceTest.test(200, step -> {
            try(IteratorEntity<RecordReadable> i = domainObjectSource.iterator(RecordReadable.class, null)) {
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

        Transaction transaction = domainObjectSource.buildTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
            rec.setString1((i % 100) == 0 ? fixedString : UUID.randomUUID().toString());
            transaction.save(rec);
        }
        transaction.commit();

        final Map<String, Object> filter = new HashMap<String, Object>(){{put(RecordIndexEditable.FIELD_STRING_1, fixedString);}};

        PerfomanceTest.test(1, step -> {
            IteratorEntity<RecordIndexEditable> i = domainObjectSource.find(RecordIndexEditable.class, null, filter);
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

        Transaction transaction = domainObjectSource.buildTransaction();
        for (int i = 0; i < recordCount; ++i) {
            RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
            rec.setLong1((i % 100) == 0 ? fixedLong : RandomUtil.random.nextLong());
            transaction.save(rec);
        }
        transaction.commit();

        final Map<String, Object> filter = new HashMap<String, Object>(){{put(RecordIndexEditable.FIELD_LONG_1, fixedLong);}};

        PerfomanceTest.test(50, step -> {
            IteratorEntity<RecordIndexEditable> i = domainObjectSource.find(RecordIndexEditable.class, null, filter);
            while (i.hasNext()) {
                RecordIndexEditable rec = i.next();
            }
        });
    }
}
