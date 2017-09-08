package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class ComboIndexIteratorRemoveDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(ComboIndexIteratorRemoveDomainObjectTest.class);

    @Test
    public void run() throws Exception {
//        RocksDataBase rocksDataBase = new RocksdbBuilder()
//                .withPath(pathDataBase)
//                .build();
//
//        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));
//
//        //Добавляем объекты
//        domainObjectSource.getEngineTransaction().execute(new Monad() {
//            @Override
//            public void action(Transaction transaction) throws Exception {
//                for (int i=1; i<=10; i++) {
//                    StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
//                    storeFile.setFileName((i%2==0)?"2":"1");
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
//        int count=0;
//        for (StoreFile storeFile: domainObjectSource.findAll(StoreFile.class, new HashMap<String, Object>(){{
//            put("size", 100L);
//            put("fileName", "1");
//        }})) {
//            count++;
//            Assert.assertNotNull(storeFile);
//            Assert.assertEquals(100, storeFile.getSize());
//            Assert.assertEquals("1", storeFile.getFileName());
//        }
//        Assert.assertEquals(4, count);
//
//
//        rocksDataBase.destroy();
    }

}
