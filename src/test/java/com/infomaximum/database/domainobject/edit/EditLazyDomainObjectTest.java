package com.infomaximum.database.domainobject.edit;

import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class EditLazyDomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName1 = "info1.json";
        String fileName2 = "info2.json";
        String contentType = "application/json";
        long size = 1000L;

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setFileName(fileName1);
                storeFile.setContentType(contentType);
                storeFile.setSize(size);
                transaction.save(storeFile);
        });

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setFileName(fileName2);
                transaction.save(storeFile);
        });

        //Загружаем отредактированный объект
        StoreFileReadable editFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(editFileCheckSave);
        Assert.assertEquals(fileName2, editFileCheckSave.getFileName());
        Assert.assertEquals(contentType, editFileCheckSave.getContentType());
        Assert.assertEquals(size, editFileCheckSave.getSize());
    }
}
