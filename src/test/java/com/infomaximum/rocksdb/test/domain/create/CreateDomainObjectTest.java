package com.infomaximum.rocksdb.test.domain.create;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.Monad;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.domain.type.FormatType;
import com.infomaximum.rocksdb.struct.RocksDataBase;
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
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName="application/json";
        String contentType="info.json";
        long size=1000L;
        FormatType format = FormatType.B;

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.create(StoreFileEditable.class);
                storeFile.setContentType(contentType);
                storeFile.setFileName(fileName);
                storeFile.setSize(size);
                storeFile.setFormat(format);
                domainObjectSource.save(transaction, storeFile);
            }
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, 1L);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(fileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());
        Assert.assertEquals(size, storeFileCheckSave.getSize());
        Assert.assertEquals(format, storeFileCheckSave.getFormat());

        rocksDataBase.destroy();
    }

}
