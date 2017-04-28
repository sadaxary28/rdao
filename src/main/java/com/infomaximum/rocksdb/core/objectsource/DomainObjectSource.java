package com.infomaximum.rocksdb.core.objectsource;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodFilterImpl;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodHandlerImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.engine.impl.EngineTransactionImpl;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public class DomainObjectSource {

    private final DataSource dataSource;
    private final EngineTransaction engineTransaction;

    private final Map<Class<? extends DomainObject>, MethodFilter> methodFilters;
    private final Map<Class<? extends DomainObject>, MethodHandler> methodHandlers;

    public DomainObjectSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.engineTransaction = new EngineTransactionImpl(dataSource);

        this.methodFilters = new HashMap<Class<? extends DomainObject>, MethodFilter>();
        this.methodHandlers = new HashMap<Class<? extends DomainObject>, MethodHandler>();
    }

    public EngineTransaction getEngineTransaction() {
        return engineTransaction;
    }


    public <T extends DomainObject> T create(final Transaction transaction, final Class<? extends DomainObject> clazz) throws ReflectiveOperationException, RocksDBException {
        if (transaction==null) throw new IllegalArgumentException("Transaction is empty");

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        long id = dataSource.nextId(entityAnnotation.columnFamily());

        T domainObject = createDomainObject(clazz, id, null);

        //Указываем транзакцию
        HashStructEntities.getTransactionField().set(domainObject, transaction);

        return domainObject;
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T edit(final Transaction transaction, final Class<? extends DomainObject> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        Map<String, byte[]> data = dataSource.lock(entityAnnotation.columnFamily(), id, null);
        if (data==null) return null;

        T domainObject = createDomainObject(clazz, id, data);

        //Указываем транзакцию
        HashStructEntities.getTransactionField().set(domainObject, transaction);

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

        Map<String, byte[]> data = dataSource.gets(entityAnnotation.columnFamily(), id, HashStructEntities.getStructEntity(clazz).getEagerFieldNames());
        if (data==null) return null;

        return createDomainObject(clazz, id, data);
    }

    private <T extends DomainObject> T createDomainObject(final Class<? extends DomainObject> clazz, long id, Map<String, byte[]> data) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);
        factory.setFilter(getMethodFilter(clazz));

        T domainObject = (T) factory.create(
                new Class<?>[]{long.class},
                new Object[]{id},
                getMethodHandler(clazz)
        );

        //Устанавливаем dataSource
        HashStructEntities.getDataSourceField().set(domainObject, dataSource);

        //Загружаем поля
        if (data!=null) {
            StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

            for (String fieldName: structEntity.getFieldNames()) {
                Field field = HashStructEntities.getStructEntity(clazz).getField(fieldName);
                if (data.containsKey(fieldName)) {
                    byte[] value = data.get(fieldName);
                    field.set(domainObject, TypeConvertRocksdb.get(field.getType(), value));
                } else {
                    Field lazyLoadsField = HashStructEntities.getLazyLoadsField();

                    Set<Field> lazyLoads = (Set<Field>) lazyLoadsField.get(domainObject);
                    if (lazyLoads==null) {
                        lazyLoads = new HashSet<Field>();
                        lazyLoadsField.set(domainObject, lazyLoads);
                    }
                    lazyLoads.add(field);
                }
            }
        }

        return domainObject;
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

}
