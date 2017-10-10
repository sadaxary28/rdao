package com.infomaximum.rocksdb.test.domain.remove;

import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class RemoveDomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 3L));


        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                transaction.save(transaction.create(StoreFileEditable.class));
                transaction.save(transaction.create(StoreFileEditable.class));
                transaction.save(transaction.create(StoreFileEditable.class));
        });

        //Проверяем что файлы сохранены
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));

        //Удяляем 2-й объект
        domainObjectSource.executeTransactional(transaction -> {
                transaction.remove(transaction.get(StoreFileEditable.class, 2L));
        });

        //Проверяем, корректность удаления
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));
    }
}
