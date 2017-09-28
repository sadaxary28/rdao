package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.ExchangeFolderEditable;
import com.infomaximum.rocksdb.domain.ExchangeFolderReadable;
import com.infomaximum.rocksdb.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by kris on 22.04.17.
 */
public class ComboIndexDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(ComboIndexDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(ExchangeFolderEditable.class);

        String uuid = "AQMkAGYzOGZhMGRlLTk0ZmQtNGU4Mi05YzMyLWU1YmMyODgAMzA1MzkALgAAA2q7G9o/e25DjV2GPrKtaxsBAOVhxnfq2u5Gj3QIHLYcQRoAAAIBDQAAAA==";
        String userEmail = "test1@infomaximum.onmicrosoft.com";

//        Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
                ExchangeFolderEditable exchangeFolder = transaction.create(ExchangeFolderEditable.class);
                exchangeFolder.setUuid(uuid);
                exchangeFolder.setUserEmail(userEmail);
                transaction.save(exchangeFolder);
        });


        //Ищем объект
         try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, null, new HashMap<String, Object>(){{
            put(ExchangeFolderReadable.FIELD_UUID, uuid);
            put(ExchangeFolderReadable.FIELD_USER_EMAIL, userEmail);
        }})) {
             ExchangeFolderReadable exchangeFolder = i.next();
             Assert.assertNotNull(exchangeFolder);
             Assert.assertEquals(uuid, exchangeFolder.getUuid());
             Assert.assertEquals(userEmail, exchangeFolder.getUserEmail());
         }

        rocksDataBase.close();
    }

}
