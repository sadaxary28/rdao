package com.infomaximum.rocksdb.core.objectsource.utils;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.rocksdb.core.iterator.IteratorFindEntity;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodFilterImpl;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodHandlerImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
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
 * Created by kris on 28.04.17.
 */
public class DomainObjectUtils {

    private static final Map<Class<? extends DomainObject>, MethodFilter> methodFilters = new HashMap<Class<? extends DomainObject>, MethodFilter>();
    private static final Map<Class<? extends DomainObject>, MethodHandler> methodHandlers = new HashMap<Class<? extends DomainObject>, MethodHandler>();


    public static <T extends DomainObject> T create(DataSource dataSource, final Transaction transaction, final Class<T> clazz) throws ReflectiveOperationException, RocksDBException {
        if (transaction==null) throw new IllegalArgumentException("Transaction is empty");

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        long id = dataSource.nextId(entityAnnotation.columnFamily());

        T domainObject = createDomainObject(dataSource, clazz, new EntitySourceImpl(id, null));

        //Указываем транзакцию
        HashStructEntities.getTransactionField().set(domainObject, transaction);

        return domainObject;
    }

    public static <T extends DomainObject> T get(DataSource dataSource, final Transaction transaction, final Class<T> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        EntitySource entitySource = dataSource.getEntitySource(entityAnnotation.columnFamily(), (transaction != null), id, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
        if (entitySource==null) return null;

        T domainObject = createDomainObject(dataSource, clazz, entitySource);

        if (transaction!=null) {
            //Указываем транзакцию
            HashStructEntities.getTransactionField().set(domainObject, transaction);
        }

        return domainObject;
    }


    public static <T extends DomainObject> IteratorFindEntity<T> findAll(DataSource dataSource, Class<T> clazz, Map<String, Object> filters) throws NoSuchMethodException, InstantiationException, RocksDBException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
        return new IteratorFindEntity(dataSource, clazz, filters);
    }

    public static <T extends DomainObject> T createDomainObject(DataSource dataSource, final Class<T> clazz, EntitySource entitySource) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, NoSuchFieldException {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(clazz);
        factory.setFilter(getMethodFilter(clazz));

        T domainObject = (T) factory.create(
                new Class<?>[]{long.class},
                new Object[]{entitySource.getId()},
                getMethodHandler(clazz)
        );

        //Устанавливаем dataSource
        HashStructEntities.getDataSourceField().set(domainObject, dataSource);

        //Загружаем поля
        Map<String, byte[]> data = entitySource.getFields();
        if (data!=null) {
            StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

            for (String formatFieldName: structEntity.getFormatFieldNames()) {
                Field field = HashStructEntities.getStructEntity(clazz).getFieldByFormatName(formatFieldName);
                if (data.containsKey(formatFieldName)) {
                    byte[] value = data.get(formatFieldName);
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

    private static MethodFilter getMethodFilter(final Class<? extends DomainObject> clazz) {
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

    private static MethodHandler getMethodHandler(final Class<? extends DomainObject> clazz) {
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
