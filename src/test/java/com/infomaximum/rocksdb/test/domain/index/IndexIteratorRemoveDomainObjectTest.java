package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.StoreFile;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.Monad;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class IndexIteratorRemoveDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexIteratorRemoveDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=1; i<=10; i++) {
                    StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
                    storeFile.setSize(100);
                    storeFile.save();
                }
            }
        });

        //Редактируем 1-й объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFile storeFile = domainObjectSource.edit(transaction, StoreFile.class, 1L);
                storeFile.setSize(99);
                storeFile.save();
            }
        });


        //Ищем объекты по size
        int count=0;
        for (StoreFile storeFile: domainObjectSource.findAll(StoreFile.class, "size", 100L)) {
            count++;
            Assert.assertNotNull(storeFile);
            Assert.assertEquals(100, storeFile.getSize());
        }
        Assert.assertEquals(9, count);


        rocksDataBase.destroy();
    }

}
