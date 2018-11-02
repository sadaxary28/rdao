package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.domainobject.DomainDataInstanceTest;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;


public class DeletePerformanceTest extends DomainDataInstanceTest {

    @Test
    public void deleteReadTest() throws Exception {
        final long objectsCount = 100_000;
        final long deletingCount = 70_000;
        final int executionTimes = 3;

        fillEtalonBD((d, r) -> createObjects(objectsCount, d, r));
        resetBDToEtalon();

        System.out.println("Чтение:");
        PerfomanceTest.test(executionTimes,
                step -> read(deletingCount, objectsCount)
                );

        System.out.println("В одной транзакции удаление и чтение:");
        resetBDToEtalon();
        domainObjectSource.executeTransactional(transaction -> {
            deleteObjects(deletingCount, transaction);
            PerfomanceTest.test(executionTimes,
                    step -> read(1L, objectsCount, transaction)
            );
        });

        System.out.println("В разных транзакциях удаление и чтение:");
        resetBDToEtalon();
        deleteObjects(deletingCount);
        PerfomanceTest.test(executionTimes,
                step -> read(1L, objectsCount)
        );
    }

    private void createObjects(long count, DomainObjectSource domainObjectSource, RocksDBProvider rocksDBProvider) throws Exception {
        createDomain(GeneralEditable.class, rocksDBProvider);
        domainObjectSource.executeTransactional(transaction -> {
            for (Long i = 1L; i <= count; i++) {
                GeneralEditable generalEditable = transaction.create(GeneralEditable.class);
                generalEditable.setValue(i);
                transaction.save(generalEditable);
            }
        });
    }

    private void deleteObjects(long count) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            deleteObjects(count, transaction);
        });
    }

    private void deleteObjects(long count, Transaction transaction) throws Exception {
        for (Long i = 1L; i <= count; i++) {
            GeneralEditable generalEditable = transaction.get(GeneralEditable.class, i);
            transaction.remove(generalEditable);
        }
    }

    private void read(Long begin, Long end) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            read(begin, end, transaction);
        });
    }

    private void read(Long begin, Long end, Transaction transaction) throws Exception {
        for (Long i = begin; i <= end; i++) {
            try (IteratorEntity<GeneralEditable> it = transaction.find(GeneralEditable.class, new HashFilter(GeneralEditable.FIELD_VALUE, i))) {
                it.hasNext();
            }
        }
    }
}