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
public class IndexRemove2DomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexRemove2DomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 1L));

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                    StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
                    storeFile.setSize(100);
                    storeFile.save();
            }
        });

        //Редактируем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFile storeFile = domainObjectSource.edit(transaction, StoreFile.class, 1L);
                storeFile.setSize(99);
                storeFile.save();
            }
        });


        //Ищем объекты по size
        StoreFile storeFile = domainObjectSource.find(StoreFile.class, "size", 100L);
        Assert.assertNull(storeFile);

        rocksDataBase.destroy();
    }

}
