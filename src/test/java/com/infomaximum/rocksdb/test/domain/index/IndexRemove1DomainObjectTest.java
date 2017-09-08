package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class IndexRemove1DomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexRemove1DomainObjectTest.class);

    @Test
    public void run() throws Exception {
//        RocksDataBase rocksDataBase = new RocksdbBuilder()
//                .withPath(pathDataBase)
//                .build();
//
//        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));
//
//        //Проверяем, что таких объектов нет в базе
//        Assert.assertNull(domainObjectSource.get(StoreFile.class, 1L));
//        Assert.assertNull(domainObjectSource.get(StoreFile.class, 2L));
//
//        //Добавляем объекты
//        domainObjectSource.getEngineTransaction().execute(new Monad() {
//            @Override
//            public void action(Transaction transaction) throws Exception {
//                for (int i=1; i<=2; i++) {
//                    StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
//                    storeFile.setSize(100);
//                    storeFile.save();
//                }
//            }
//        });
//
//        //Редактируем 1-й объект
//        domainObjectSource.getEngineTransaction().execute(new Monad() {
//            @Override
//            public void action(Transaction transaction) throws Exception {
//                StoreFile storeFile = domainObjectSource.edit(transaction, StoreFile.class, 1L);
//                storeFile.setSize(99);
//                storeFile.save();
//            }
//        });
//
//
//        //Ищем объекты по size
//        StoreFile storeFile = domainObjectSource.find(StoreFile.class, "size", 100L);
//        Assert.assertNotNull(storeFile);
//        Assert.assertEquals(100, storeFile.getSize());
//
//        rocksDataBase.destroy();
    }

}
