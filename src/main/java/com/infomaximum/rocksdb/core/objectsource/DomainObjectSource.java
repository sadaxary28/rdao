package com.infomaximum.rocksdb.core.objectsource;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodFilterImpl;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodHandlerImpl;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.ClassPool;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 19.04.2017.
 */
public class DomainObjectSource {

    private final DataSource dataSource;

    private Map<Class<? extends DomainObject>, MethodFilter> methodFilters;
    private Map<Class<? extends DomainObject>, MethodHandler> methodHandlers;

    public DomainObjectSource(DataSource dataSource) {
        this.dataSource = dataSource;

        this.methodFilters = new HashMap<Class<? extends DomainObject>, MethodFilter>();
        this.methodHandlers = new HashMap<Class<? extends DomainObject>, MethodHandler>();
    }

    public <T extends DomainObject> T create(final Class<? extends DomainObject> clazz) throws ReflectiveOperationException, RocksDBException {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        long id = dataSource.nextId(entityAnnotation.columnFamily());

        ClassPool classPool = ClassPool.getDefault();

        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);
        factory.setFilter(getMethodFilter(clazz));

        T domainObject = (T) factory.create(
                new Class<?>[]{long.class},
                new Object[]{id},
                getMethodHandler(clazz)
        );

        //Снимаем флаг readonly
        setReadOnly(domainObject, false);

        return domainObject;
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T get(final Class<? extends DomainObject> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        Map<String, byte[]> data = dataSource.load(entityAnnotation.columnFamily(), id, true);
        if (data==null) return null;

        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);
        factory.setFilter(getMethodFilter(clazz));

        return (T) factory.create(
                new Class<?>[]{long.class},
                new Object[]{id},
                getMethodHandler(clazz)
        );
    }

    private MethodFilter getMethodFilter(final Class<? extends DomainObject> clazz) {
        MethodFilter methodFilter = methodFilters.get(clazz);
        if (methodFilter==null) {
            synchronized (methodFilters) {
                methodFilter = methodFilters.get(clazz);
                if (methodFilter==null) {
                    methodFilter = new MethodFilterImpl(clazz);
                    methodFilters.put(clazz, methodFilter);
                }
            }
        }
        return methodFilter;
    }

    private MethodHandler getMethodHandler(final Class<? extends DomainObject> clazz) {
        MethodHandler methodHandler = methodHandlers.get(clazz);
        if (methodHandler==null) {
            synchronized (methodFilters) {
                methodHandler = methodHandlers.get(clazz);
                if (methodHandler==null) {
                    methodHandler = new MethodHandlerImpl(clazz);
                    methodHandlers.put(clazz, methodHandler);
                }
            }
        }
        return methodHandler;
    }

    private static void setReadOnly(DomainObject domainObject, boolean readOnly) throws NoSuchFieldException, IllegalAccessException {
        //таким способом не находит поле
        //domainObject.getClass().getSuperclass().getSuperclass().getDeclaredFields()
        Field fieldReadOnly = domainObject.getClass().getField("readOnly");
        fieldReadOnly.set(domainObject, readOnly);
    }
}
