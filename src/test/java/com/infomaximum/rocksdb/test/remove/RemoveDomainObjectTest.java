package com.infomaximum.rocksdb.test.remove;

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
public class RemoveDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(RemoveDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 2L));
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 3L));


        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                domainObjectSource.create(transaction, StoreFile.class).save();
                domainObjectSource.create(transaction, StoreFile.class).save();
                domainObjectSource.create(transaction, StoreFile.class).save();
            }
        });

        //Проверяем что файлы сохранены
        Assert.assertNotNull(domainObjectSource.get(StoreFile.class, 1L));
        Assert.assertNotNull(domainObjectSource.get(StoreFile.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFile.class, 3L));

        //Удяляем 2-й объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                domainObjectSource.edit(transaction, StoreFile.class, 2L).remove();
            }
        });

        //Проверяем, корректность удаления
        Assert.assertNotNull(domainObjectSource.get(StoreFile.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFile.class, 3L));

        rocksDataBase.destroy();
    }

}
