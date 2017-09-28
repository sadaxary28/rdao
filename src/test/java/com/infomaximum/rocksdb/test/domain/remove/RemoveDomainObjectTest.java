package com.infomaximum.rocksdb.test.domain.remove;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.RocksDataBase;
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
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);

        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, null, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, null, 2L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, null, 3L));


        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                transaction.save(transaction.create(StoreFileEditable.class));
                transaction.save(transaction.create(StoreFileEditable.class));
                transaction.save(transaction.create(StoreFileEditable.class));
        });

        //Проверяем что файлы сохранены
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, null, 1L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, null, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, null, 3L));

        //Удяляем 2-й объект
        domainObjectSource.executeTransactional(transaction -> {
                transaction.remove(transaction.get(StoreFileEditable.class, null, 2L));
        });

        //Проверяем, корректность удаления
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, null, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, null, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, null, 3L));

        rocksDataBase.close();
    }
}
