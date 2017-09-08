package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.rocksdb.RocksDataTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kris on 22.04.17.
 */
public class NullValueDependenceDomainObjectTest extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(NullValueDependenceDomainObjectTest.class);

    @Test
    public void run() throws Exception {
//        RocksDataBase rocksDataBase = new RocksdbBuilder()
//                .withPath(pathDataBase)
//                .build();
//
//        DomainObjectSource domainObjectSource = new DomainObjectSource(new DataSourceImpl(rocksDataBase));
//
//        //Добавляем объекты
//        domainObjectSource.getEngineTransaction().execute(new Monad() {
//            @Override
//            public void action(Transaction transaction) throws Exception {
//                //Создали первый объект
//                Department department1 = domainObjectSource.create(transaction, Department.class);
//                department1.setName("department1");
//                department1.save();
//
//                Department department2 = domainObjectSource.create(transaction, Department.class);
//                department2.setParent(department1);
//                department2.setName("department2");
//                department2.save();
//            }
//        });
//
//        //Редактируем 2-й объект
//        domainObjectSource.getEngineTransaction().execute(new Monad() {
//            @Override
//            public void action(Transaction transaction) throws Exception {
//                Department department2 = domainObjectSource.edit(transaction, Department.class, 2);
//                department2.setParent(null);
//                department2.save();
//            }
//        });
//
//        //Ищем по null
//        int count=0;
//        for (Department iDepartment: domainObjectSource.findAll(Department.class, "parent", null)) {
//            Assert.assertNotNull(iDepartment);
//            Assert.assertNull(iDepartment.getParent());
//            count++;
//        }
//        Assert.assertEquals(2, count);
//
//        rocksDataBase.destroy();
    }

}
