package com.infomaximum.rocksdb.test.domain.iterator;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.Monad;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.test.domain.create.CreateDomainObjectTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntityTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(CreateDomainObjectTest.class);

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();
        DomainObjectSource domainObjectSource = new DomainObjectSource(new RocksDBDataSourceImpl(rocksDataBase));
        domainObjectSource.createEntity(StoreFileReadable.class);


        try (IteratorEntity iteratorEmpty = domainObjectSource.iterator(StoreFileReadable.class)){
            Assert.assertFalse(iteratorEmpty.hasNext());
            try {
                iteratorEmpty.next();
                Assert.fail();
            } catch (NoSuchElementException e){}
        }


        int size=10;

        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=0; i< size; i++) {
                    domainObjectSource.save(domainObjectSource.create(StoreFileEditable.class), transaction);
                }
            }
        });


        //Итератором пробегаемся
        int count = 0;
        long prevId=0;
        try (IteratorEntity<StoreFileReadable> iStoreFileReadable = domainObjectSource.iterator(StoreFileReadable.class)) {
            while(iStoreFileReadable.hasNext()) {
                StoreFileReadable storeFile = iStoreFileReadable.next();

                count++;

                if (prevId==storeFile.getId()) Assert.fail("Fail next object");
                if (prevId>=storeFile.getId()) Assert.fail("Fail sort id to iterators");
                prevId=storeFile.getId();
            }

        }
        Assert.assertEquals(size, count);

        rocksDataBase.close();
    }
}
