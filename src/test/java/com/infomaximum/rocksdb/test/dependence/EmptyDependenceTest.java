package com.infomaximum.rocksdb.test.dependence;

import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.core.datasource.DataSourceImpl;
import com.infomaximum.rocksdb.core.objectsource.DomainObjectSource;
import com.infomaximum.rocksdb.domain.Department;
import com.infomaximum.rocksdb.domain.StoreFile;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.Monad;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kris on 28.04.17.
 */
public class EmptyDependenceTest extends RocksDataTest {

    @Test
    public void run() throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));

        String name = "IT";

        //Добавляем объект
        domainObjectSource.getEngineTransaction().execute(new Monad() {
            @Override
            public void action(Transaction transaction) throws Exception {
                Department department = domainObjectSource.create(transaction, Department.class);
                department.setName(name);
                department.save();
            }
        });

        //Загружаем сохраненый объект
        Department departmentCheckSave = domainObjectSource.get(Department.class, 1L);
        Assert.assertNotNull(departmentCheckSave);
        Assert.assertEquals(name, departmentCheckSave.getName());
        Assert.assertNull(departmentCheckSave.getParent());

        rocksDataBase.destroy();
    }
}
