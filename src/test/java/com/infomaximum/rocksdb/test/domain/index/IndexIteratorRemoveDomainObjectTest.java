package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.RocksDataBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by kris on 22.04.17.
 */
public class IndexIteratorRemoveDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(IndexIteratorRemoveDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);

        //Добавляем объекты
        domainObjectSource.executeTransactional(transaction -> {
                for (int i=1; i<=10; i++) {
                    StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                    storeFile.setSize(100);
                    transaction.save(storeFile);
                }
        });

        //Редактируем 1-й объект
        domainObjectSource.executeTransactional(transaction -> {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, null, 1L);
                storeFile.setSize(99);
                transaction.save(storeFile);
        });

        //Ищем объекты по size
        int count=0;
        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.find(StoreFileReadable.class, null, new HashMap<String, Object>() {{
            put(StoreFileReadable.FIELD_SIZE, 100L);
        }})) {
            while(iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                Assert.assertNotNull(storeFile);
                Assert.assertEquals(100, storeFile.getSize());

                count++;
            }
        }
        Assert.assertEquals(9, count);

        rocksDataBase.close();
    }
}
