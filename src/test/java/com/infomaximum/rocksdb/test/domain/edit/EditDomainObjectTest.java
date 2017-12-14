package com.infomaximum.rocksdb.test.domain.edit;

import com.infomaximum.database.domainobject.Transaction;
import com.infomaximum.database.exeption.ForeignDependencyException;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.domain.type.FormatType;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class EditDomainObjectTest extends StoreFileDataTest {

    @Test
    public void main() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName1 = "info1.json";
        String fileName2 = "info2.json";
        String contentType = "application/json";
        long size = 1000L;

        //Добавляем объект
        try (Transaction transaction = domainObjectSource.buildTransaction()) {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setFileName(fileName1);
            storeFile.setContentType(contentType);
            storeFile.setSize(size);
            storeFile.setSingle(false);
            transaction.save(storeFile);
            transaction.commit();
        }

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName1, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertEquals(false, storeFileCheckSave.isSingle());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, 1L);
                obj.setFileName(fileName2);
                obj.setSingle(true);
                transaction.save(obj);
        });

        //Загружаем отредактированный объект
        StoreFileReadable editFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(editFileCheckSave);
        Assert.assertEquals(fileName2, editFileCheckSave.getFileName());
        Assert.assertEquals(contentType, editFileCheckSave.getContentType());
        Assert.assertEquals(size, editFileCheckSave.getSize());
        Assert.assertEquals(true, editFileCheckSave.isSingle());


        //Повторно редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, 1L);
                obj.setFileName(fileName1);
                obj.setSingle(false);
                transaction.save(obj);
        });


        StoreFileReadable storeFileCheckSave2 = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave2);
        Assert.assertEquals(fileName1, storeFileCheckSave2.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave2.getContentType());
        Assert.assertEquals(size, storeFileCheckSave2.getSize());
        Assert.assertEquals(false, storeFileCheckSave2.isSingle());
    }

    @Test
    public void updateByNonExistenceObject() throws Exception {
        createDomain(ExchangeFolderReadable.class);

        try {
            domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable file = transaction.create(StoreFileEditable.class);
                file.setFolderId(256);
                transaction.save(file);
            });
            Assert.fail();
        } catch (ForeignDependencyException ex) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void valueStringEmptyThenNull() throws Exception {
        final long objectId = 1;
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(emptyFileName);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(emptyFileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, objectId);
            obj.setContentType(null);
            transaction.save(obj);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckEdit = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckEdit);
        Assert.assertEquals(emptyFileName, storeFileCheckEdit.getFileName());
        Assert.assertNull(storeFileCheckEdit.getContentType());
    }
}
