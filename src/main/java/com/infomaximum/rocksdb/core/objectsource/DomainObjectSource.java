package com.infomaximum.rocksdb.core.objectsource;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.util.proxy.ProxyFactory;
import org.rocksdb.RocksDBException;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * Created by user on 19.04.2017.
 */
public class DomainObjectSource {

    private final DataSource dataSource;

    public DomainObjectSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T get(final Class<? extends DomainObject> clazz, long id) throws RocksDBException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        Map<String, byte[]> data = dataSource.load(entityAnnotation.columnFamily(), id, true);
        if (data==null) return null;

        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);

        T domainObject = (T) factory.create(
                new Class<?>[]{long.class},
                new Object[]{id},
                new DomainObjectMethodHandler()
        );

        return domainObject;
    }
}
