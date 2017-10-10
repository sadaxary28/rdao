package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.IndexFilter;
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

import java.util.HashMap;

/**
 * Created by kris on 22.04.17.
 */
public class IndexRemove2DomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexRemove2DomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);

        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(100);
                transaction.save(storeFile);
        });

        //Редактируем объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                transaction.save(storeFile);
        });

        //Ищем объекты по size
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new IndexFilter(StoreFileReadable.FIELD_SIZE, 100L))) {
            Assert.assertFalse(i.hasNext());
        }

        rocksDataBase.close();
    }
}
