package com.infomaximum.rocksdb.test.domain.edit;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.StoreFile;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.Monad;
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

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 1L));

        String fileName1 = "info1.json";
        String fileName2 = "info2.json";
        String contentType = "application/json";
        long size = 1000L;

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
                storeFile.setFileName(fileName1);
                storeFile.setContentType(contentType);
                storeFile.setSize(size);
                storeFile.save();
            }
        });

        //Редактируем сохраненый объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFile storeFile = domainObjectSource.edit(transaction, StoreFile.class, 1L);
                storeFile.setFileName(fileName2);
                storeFile.save();
            }
        });

        //Загружаем отредактированный объект
        StoreFile editFileCheckSave = domainObjectSource.get(StoreFile.class, 1L);
        Assert.assertNotNull(editFileCheckSave);
        Assert.assertEquals(fileName2, editFileCheckSave.getFileName());
        Assert.assertEquals(contentType, editFileCheckSave.getContentType());
        Assert.assertEquals(size, editFileCheckSave.getSize());

        rocksDataBase.destroy();
    }

}
