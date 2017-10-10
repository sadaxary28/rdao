package com.infomaximum.rocksdb.test.domain.transaction;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import org.junit.Assert;
import org.junit.Test;

public class OptimisticTransactionLazyTest extends RocksDataTest {


    @Test
    public void test() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {

            DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
            domainObjectSource.createEntity(StoreFileReadable.class);

            StructEntity structEntityStoreFileReadable = StructEntity.getInstance(StoreFileReadable.class);

            String fileName = "aaa.txt";
            long size = 15L;

            domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile1 = transaction.create(StoreFileEditable.class);
                storeFile1.setFileName(fileName);
                storeFile1.setSize(size);
                transaction.save(storeFile1);

                long iteratorId = transaction.createIterator(structEntityStoreFileReadable.getName(), null);
                try {
                    StoreFileReadable storeFile2 = DomainObjectUtils.nextObject(StoreFileReadable.class, transaction, iteratorId, null);

                    Assert.assertEquals(fileName, storeFile2.getFileName());
                    Assert.assertEquals(size, storeFile2.getSize());
                } finally {
                    transaction.closeIterator(iteratorId);
                }
            });
        }
    }
}
