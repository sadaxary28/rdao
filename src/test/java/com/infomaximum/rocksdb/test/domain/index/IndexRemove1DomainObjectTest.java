package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class IndexRemove1DomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));

        //Добавляем объекты
        domainObjectSource.executeTransactional(transaction -> {
                for (int i=1; i<=2; i++) {
                    StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                    storeFile.setSize(100);
                    transaction.save(storeFile);
                }
        });

        //Редактируем 1-й объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                transaction.save(storeFile);
        });

        //Ищем объекты по size
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new IndexFilter("size", 100L))) {
            StoreFileReadable storeFile = i.next();
            Assert.assertNotNull(storeFile);
            Assert.assertEquals(100, storeFile.getSize());
            Assert.assertFalse(i.hasNext());
        }
    }
}
