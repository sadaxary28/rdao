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
public class IndexRemove2DomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexRemove2DomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.create(StoreFileEditable.class);
                storeFile.setSize(100);
                domainObjectSource.save(transaction, storeFile);
            }
        });

        //Редактируем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                domainObjectSource.save(transaction, storeFile);
            }
        });


        //Ищем объекты по size
        StoreFileReadable storeFile = domainObjectSource.find(StoreFileReadable.class, StoreFileReadable.FIELD_SIZE, 100L);
        Assert.assertNull(storeFile);

        rocksDataBase.destroy();
    }

}
