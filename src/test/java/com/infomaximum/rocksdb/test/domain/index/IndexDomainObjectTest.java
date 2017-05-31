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
public class IndexDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Проверяем, что таких объектов нет в базе
        for (long i=1; i<=100; i++) {
            Assert.assertNull(domainObjectSource.get(StoreFile.class, i));
            Assert.assertNull(domainObjectSource.find(StoreFile.class, "size", i));
        }


        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=1; i<=100; i++) {
                    StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
                    storeFile.setSize(i);
                    storeFile.save();
                }
            }
        });

        //Проверяем что файлы сохранены
        for (long id=1; id<=100; id++) {
            Assert.assertNotNull(domainObjectSource.get(StoreFile.class, id));
        }

        //Ищем объекты по size
        for (long size=1; size<=100; size++) {
            StoreFile storeFile = domainObjectSource.find(StoreFile.class, "size", size);
            Assert.assertNotNull(storeFile);
            Assert.assertEquals(size, storeFile.getSize());
        }

        rocksDataBase.destroy();
    }

}
