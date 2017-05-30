package com.infomaximum.rocksdb.core.objectsource.utils;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.rocksdb.core.iterator.IteratorFindEntity;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodFilterImpl;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodHandlerImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.index.IndexUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityIndex;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.EqualsUtils;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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


    public static <T extends DomainObject> T find(DataSource dataSource, Transaction transaction, Class<T> clazz, String fieldName, Object value) throws RocksDBException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException, InvocationTargetException {
        StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

        Field field = structEntity.getFieldByName(fieldName);
        if (field==null) throw new RuntimeException("Not found field " + fieldName + ", to " + clazz.getName());
        if (!EqualsUtils.equalsType(field.getType(), value.getClass())) throw new RuntimeException("Not equals type field " + field.getType() + " and type value " + value.getClass());
        Method getterToField = structEntity.getGetterMethodByField(field);

        String nameIndex = StructEntityIndex.buildNameIndex(field);

        int hash = IndexUtils.calcHashValue(value);

        Long prevId = null;
        T domainObject;
        while (true) {
            EntitySource entitySource = dataSource.findNextEntitySource(structEntity.annotationEntity.columnFamily(), prevId, (transaction != null), nameIndex, hash, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
            if (entitySource==null) return null;

            domainObject = createDomainObject(dataSource, clazz, entitySource);

            //Необходима дополнительная проверка, так как нельзя исключать сломанный индекс или коллизии хеша
            Object iValue = getterToField.invoke(domainObject);
            if (EqualsUtils.equals(iValue, value)) {
                //Все хорошо, совпадение полное - выходим
                break;
            } else {
                //Промахнулись с индексом - уходим на повторный круг
                prevId = domainObject.getId();
            }
        }


        if (transaction!=null) {
            //Указываем транзакцию
            HashStructEntities.getTransactionField().set(domainObject, transaction);
        }

        return domainObject;
    }

    public static <T extends DomainObject> IteratorFindEntity<T> findAll(DataSource dataSource, Transaction transaction, Class<T> clazz, String fieldName, Object value) throws RocksDBException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException, InvocationTargetException {
        StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

        Field field = structEntity.getFieldByName(fieldName);
        if (field==null) throw new RuntimeException("Not found field " + fieldName + ", to " + clazz.getName());
        if (!EqualsUtils.equalsType(field.getType(), value.getClass())) throw new RuntimeException("Not equals type field " + field.getType() + " and type value " + value.getClass());
        Method getterToField = structEntity.getGetterMethodByField(field);

        String nameIndex = StructEntityIndex.buildNameIndex(field);

        int hash = IndexUtils.calcHashValue(value);

        Long prevId = null;
        T domainObject;
        while (true) {
            EntitySource entitySource = dataSource.findNextEntitySource(structEntity.annotationEntity.columnFamily(), prevId, (transaction != null), nameIndex, hash, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
            if (entitySource==null) return null;

            domainObject = createDomainObject(dataSource, clazz, entitySource);

            //Необходима дополнительная проверка, так как нельзя исключать сломанный индекс или коллизии хеша
            Object iValue = getterToField.invoke(domainObject);
            if (EqualsUtils.equals(iValue, value)) {
                //Все хорошо, совпадение полное - выходим
                break;
            } else {
                //Промахнулись с индексом - уходим на повторный круг
                prevId = domainObject.getId();
            }
        }


        if (transaction!=null) {
            //Указываем транзакцию
            HashStructEntities.getTransactionField().set(domainObject, transaction);
        }

        return null;
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
