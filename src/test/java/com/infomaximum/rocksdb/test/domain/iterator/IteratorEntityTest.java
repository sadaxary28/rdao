package com.infomaximum.rocksdb.test.domain.iterator;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.Department;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.test.domain.create.CreateDomainObjectTest;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.Monad;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
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
        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));


        Iterator iteratorEmpty = domainObjectSource.iterator(Department.class);
        Assert.assertFalse(iteratorEmpty.hasNext());
        try {
            iteratorEmpty.next();
            Assert.fail();
        } catch (NoSuchElementException e){}

        int size=10;

        //Добавляем объекты
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                for (int i=0; i< size; i++) {
                    domainObjectSource.create(transaction, Department.class).save();
                }
            }
        });


        //Итератором пробегаемся
        int count = 0;
        long prevId=0;
        for (Department department: domainObjectSource.iterator(Department.class)) {
            count++;

            if (prevId==department.getId()) Assert.fail("Fail next object");
            if (prevId>=department.getId()) Assert.fail("Fail sort id to iterators");
            prevId=department.getId();
        }
        Assert.assertEquals(size, count);

        rocksDataBase.destroy();
    }
}
