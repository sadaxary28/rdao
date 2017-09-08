package com.infomaximum.rocksdb.test.domain.edit;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.Monad;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class EditLazyDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(EditLazyDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));

        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName1 = "info1.json";
        String fileName2 = "info2.json";
        String contentType = "application/json";
        long size = 1000L;

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.create(StoreFileEditable.class);
                storeFile.setFileName(fileName1);
                storeFile.setContentType(contentType);
                storeFile.setSize(size);
                domainObjectSource.save(storeFile, transaction);
            }
        });

        //Редактируем сохраненый объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setFileName(fileName2);
                domainObjectSource.save(storeFile, transaction);
            }
        });

        //Загружаем отредактированный объект
        StoreFileReadable editFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(editFileCheckSave);
        Assert.assertEquals(fileName2, editFileCheckSave.getFileName());
        Assert.assertEquals(contentType, editFileCheckSave.getContentType());
        Assert.assertEquals(size, editFileCheckSave.getSize());

        rocksDataBase.destroy();
    }

}
