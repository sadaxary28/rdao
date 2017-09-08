package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.Monad;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.struct.RocksDataBase;
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

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=1; i<=10; i++) {
                    StoreFileEditable storeFile = domainObjectSource.create(StoreFileEditable.class);
                    storeFile.setSize(100);
                    domainObjectSource.save(storeFile, transaction);
                }
            }
        });

        //Редактируем 1-й объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                domainObjectSource.save(storeFile, transaction);
            }
        });


        //Ищем объекты по size
        int count=0;
        for (StoreFileReadable storeFile: domainObjectSource.findAll(StoreFileReadable.class, StoreFileReadable.FIELD_SIZE, 100L)) {
            count++;
            Assert.assertNotNull(storeFile);
            Assert.assertEquals(100, storeFile.getSize());
        }
        Assert.assertEquals(9, count);


        rocksDataBase.destroy();
    }

}
