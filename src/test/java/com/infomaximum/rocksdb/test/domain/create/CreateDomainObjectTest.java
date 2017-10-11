package com.infomaximum.rocksdb.test.domain.create;

import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.domain.type.FormatType;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 22.04.17.
 */
public class CreateDomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName="application/json";
        String contentType="info.json";
        long size=1000L;
        FormatType format = FormatType.B;

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setContentType(contentType);
                storeFile.setFileName(fileName);
                storeFile.setSize(size);
                storeFile.setFormat(format);
                transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertEquals(format, storeFileCheckSave.getFormat());
    }
}
