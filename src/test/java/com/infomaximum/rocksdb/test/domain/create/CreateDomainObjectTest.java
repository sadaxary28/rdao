package com.infomaximum.rocksdb.test.domain.create;

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
public class CreateDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(CreateDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFile.class, 1L));

        String fileName="application/json";
        String contentType="info.json";
        long size=1000L;

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFile storeFile = domainObjectSource.create(transaction, StoreFile.class);
                storeFile.setContentType(contentType);
                storeFile.setFileName(fileName);
                storeFile.setSize(size);
                storeFile.save();
            }
        });

        //Загружаем сохраненый объект
        StoreFile storeFileCheckSave = domainObjectSource.get(StoreFile.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());

        rocksDataBase.destroy();
    }

}
