package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.Monad;
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
public class ComboIndexIteratorRemoveDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(ComboIndexIteratorRemoveDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksDataBaseBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);

        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=1; i<=10; i++) {
                    StoreFileEditable storeFile = domainObjectSource.create(StoreFileEditable.class);
                    storeFile.setFileName((i%2==0)?"2":"1");
                    storeFile.setSize(100);
                    domainObjectSource.save(storeFile, transaction);
                }
            }
        });

        //Редактируем 1-й объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                StoreFileEditable storeFile = domainObjectSource.get(StoreFileEditable.class, 1L);
                storeFile.setSize(99);
                domainObjectSource.save(storeFile, transaction);
            }
        });


        //Ищем объекты по size
        int count=0;
        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.findAll(StoreFileReadable.class, new HashMap<String, Object>(){{
            put(StoreFileReadable.FIELD_SIZE, 100L);
            put(StoreFileReadable.FIELD_FILE_NAME, "1");
        }})) {
            while(iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                Assert.assertNotNull(storeFile);
                Assert.assertEquals(100, storeFile.getSize());
                Assert.assertEquals("1", storeFile.getFileName());

                count++;
            }

        }
        Assert.assertEquals(4, count);


        rocksDataBase.close();
    }

}
