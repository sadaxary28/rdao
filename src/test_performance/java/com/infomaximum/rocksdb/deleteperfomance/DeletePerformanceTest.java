package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.domainobject.DomainDataInstanceTest;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;


public class DeletePerformanceTest extends DomainDataInstanceTest {

    @Test
    public void deleteReadTest() throws Exception {
        final int generalCount = 1;
        final int generalDependenceCount = 100000;
        final int deletingDependentCount = 9000;
        final int readTimes = 100000;
        final int executionTimes = 3;

        fillEtalonDB(generalCount, generalDependenceCount);

        System.out.println("Read:");
        PerfomanceTest.test(executionTimes,
                this::resetBDToEtalon,
                step -> read(generalCount, readTimes));

        System.out.println("Read after Delete:");
        PerfomanceTest.test(executionTimes,
                () -> {
                    resetBDToEtalon();
                    deleteZebraDependent(generalCount, deletingDependentCount);
                },
                step -> read(generalCount, readTimes));
    }

    @Test
    public void writeDeleteTest() throws Exception {
        final int generalCount = 500;
        final int generalDependenceCount = 10000;
        final int deletingDependentCount = 9000;
        final int executionTimes = 3;

        PerfomanceTest.test(executionTimes,
                step -> {
                    createGeneralsAndDependents(generalCount, generalDependenceCount, domainObjectSource, rocksDBProvider);
                    deleteZebraDependent(generalCount, deletingDependentCount);
                });
    }


    private void deleteZebraDependent(int generalCount, int deletingDependent) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (long i = 1; i <= generalCount; i += 2) {
                int c = 1;
                try (IteratorEntity<DependentEditable> depIt = transaction.find(DependentEditable.class, new HashFilter(DependentEditable.FIELD_GENERAL_ID, i))) {
                    while (depIt.hasNext() && c < deletingDependent) {
                        transaction.remove(depIt.next());
                        c++;
                    }
                }
            }
        });
    }

    private void read(int generalCount, int readTimes) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int rT = 0; rT < readTimes; rT++) {
                for (long i = 1; i <= generalCount; i++) {
                    try (IteratorEntity<DependentEditable> depIt = transaction.find(DependentEditable.class, new HashFilter(DependentEditable.FIELD_GENERAL_ID, i))) {
                        if (depIt.hasNext()) {
                            depIt.next();
                        }
                    }
                }
            }
        });
    }

    private void fillEtalonDB(int generalCount, int generalDependenceCount) throws Exception {
        fillEtalonBD((domainObjectSource, rocksDBProvider) ->
                createGeneralsAndDependents(generalCount, generalDependenceCount, domainObjectSource, rocksDBProvider));
    }

    private void createGeneralsAndDependents(int generalCount, int generalDependenceCount, DomainObjectSource domainObjectSource, RocksDBProvider provider) throws Exception {
        createDomain(GeneralEditable.class, provider);
        createDomain(DependentEditable.class, provider);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < generalCount; ++i) {
                GeneralEditable generalEditable = transaction.create(GeneralEditable.class);
                generalEditable.setValue("value:" + i);
                transaction.save(generalEditable);

                for (int j = 0; j < generalDependenceCount; j++) {
                    DependentEditable dependentEditable = transaction.create(DependentEditable.class);
                    dependentEditable.setName("name:" + j);
                    dependentEditable.setGeneralId(generalEditable.getId());
                    transaction.save(dependentEditable);
                }
            }
        });
    }
}
