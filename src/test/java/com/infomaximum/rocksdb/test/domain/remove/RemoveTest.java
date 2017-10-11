package com.infomaximum.rocksdb.test.domain.remove;

import com.infomaximum.database.exeption.ForeignDependencyException;
import com.infomaximum.database.exeption.TransactionDatabaseException;
import com.infomaximum.rocksdb.domain.ExchangeFolderEditable;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class RemoveTest extends StoreFileDataTest {

    @Test
    public void removeOneObject() throws Exception {
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

    @Test
    public void removeReferencedObject() throws Exception {
        createDomain(ExchangeFolderReadable.class);

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);
        });

        try {
            domainObjectSource.executeTransactional(transaction -> {
                transaction.remove(transaction.get(ExchangeFolderEditable.class, 1));
            });
            Assert.fail();
        } catch (TransactionDatabaseException ex) {
            Assert.assertEquals(ForeignDependencyException.class, ex.getCause().getClass());
        }
    }

    @Test
    public void removeReferencingObjects() throws Exception {
        createDomain(ExchangeFolderReadable.class);

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);
        });

        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, 1));
            transaction.remove(transaction.get(ExchangeFolderEditable.class, 1));
        });
    }
}
