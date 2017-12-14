package com.infomaximum.rocksdb.test.domain.set;

import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.domain.type.FormatType;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class SetValueDomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName = "";
        String contentType1 = "info.json";
        String contentType2 = "";
        long size = 1000L;
        FormatType format = FormatType.B;

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType1);
            storeFile.setFileName(fileName);
            storeFile.setSize(size);
            storeFile.setFormat(format);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType1, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertEquals(format, storeFileCheckSave.getFormat());


        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, 1L);
            obj.setContentType(contentType2);
            obj.setSingle(true);
            transaction.save(obj);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckEdit = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckEdit);
        Assert.assertEquals(fileName, storeFileCheckEdit.getFileName());
        Assert.assertEquals(contentType2, storeFileCheckEdit.getContentType());
        Assert.assertEquals(size, storeFileCheckEdit.getSize());
        Assert.assertEquals(format, storeFileCheckEdit.getFormat());
    }
}
