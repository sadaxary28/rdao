package com.infomaximum.rocksdb;

import com.infomaximum.database.domainobject.DomainDataTest;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import com.infomaximum.util.RandomUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ReadTest extends DomainDataTest {

    private static final Set<String> preloaded = new HashSet<>(Arrays.asList(
            RecordIndexEditable.FIELD_STRING_1,
            RecordIndexEditable.FIELD_LONG_1,
            RecordIndexEditable.FIELD_BOOLEAN_1,
            RecordIndexEditable.FIELD_INT_1));

    @Test
    public void iterateRecords() throws Exception {
        final int recordCount = 50 * 1000;

        createDomain(RecordReadable.class);

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            for (int i = 0; i < recordCount; ++i) {
                RecordEditable rec = transaction.create(RecordEditable.class);
                rec.setString1("some value");
                transaction.save(rec);
            }
            transaction.commit();
        }

        PerfomanceTest.test(200, step -> {
            try (IteratorEntity<RecordReadable> i = domainObjectSource.find(RecordReadable.class, EmptyFilter.INSTANCE, preloaded)) {
                while (i.hasNext()) {
                    RecordReadable rec = i.next();
                }
            }
        });
    }

    @Test
    public void findAllByStringIndexedRecords() throws Exception {
        final int recordCount = 100 * 1000;

        createDomain(RecordIndexReadable.class);

        final String fixedString = "some value";

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            for (int i = 0; i < recordCount; ++i) {
                RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
                rec.setString1((i % 100) == 0 ? fixedString : UUID.randomUUID().toString());
                transaction.save(rec);
            }
            transaction.commit();
        }

        final HashFilter filter = new HashFilter(RecordIndexEditable.FIELD_STRING_1, fixedString);

        PerfomanceTest.test(1, step -> {
            try (IteratorEntity<RecordIndexEditable> i = domainObjectSource.find(RecordIndexEditable.class, filter, preloaded)) {
                while (i.hasNext()) {
                    RecordIndexEditable rec = i.next();
                }
            }
        });
    }

    @Test
    public void findAllByLongIndexedRecords() throws Exception {
        final int recordCount = 100 * 1000;

        createDomain(RecordIndexReadable.class);

        final long fixedLong = 500;

        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            for (int i = 0; i < recordCount; ++i) {
                RecordIndexEditable rec = transaction.create(RecordIndexEditable.class);
                rec.setLong1((i % 10) == 0 ? fixedLong : RandomUtil.random.nextLong());
                transaction.save(rec);
            }
            transaction.commit();
        }

        final HashFilter filter = new HashFilter(RecordIndexEditable.FIELD_LONG_1, fixedLong);

        PerfomanceTest.test(1000, step -> {
            try (IteratorEntity<RecordIndexEditable> i = domainObjectSource.find(RecordIndexEditable.class, filter, preloaded)) {
                while (i.hasNext()) {
                    RecordIndexEditable rec = i.next();
                }
            }
        });
    }
}
