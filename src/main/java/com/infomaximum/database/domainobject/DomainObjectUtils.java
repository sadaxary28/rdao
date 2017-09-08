package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Created by kris on 28.04.17.
 */
public class DomainObjectUtils {

//    private static final Map<Class<? extends DomainObject>, MethodFilter> methodFilters = new HashMap<Class<? extends DomainObject>, MethodFilter>();
//    private static final Map<Class<? extends DomainObject>, MethodHandler> methodHandlers = new HashMap<Class<? extends DomainObject>, MethodHandler>();


//    public static <T extends DomainObject & DomainObjectEditable> T create(DataSource dataSource, final Class<T> clazz) throws ReflectiveOperationException, RocksDBException {
//        Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);
//
//        long id = dataSource.nextId(entityAnnotation.name());
//
//        T domainObject = createDomainObject(dataSource, clazz, new EntitySourceImpl(id, null));
//
//        //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
//        for (Field field: entityAnnotation.fields()) {
//            domainObject.set(field.name(), null);
//        }
//
//        return domainObject;
//    }

//    public static <T extends DomainObject> T get(DataSource dataSource, final Class<T> clazz, long id) throws ReflectiveOperationException, RocksDBException {
//        Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);
//
//        EntitySource entitySource = dataSource.getEntitySource(entityAnnotation.name(), false, id, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
//        if (entitySource==null) return null;
//
//        T domainObject = createDomainObject(dataSource, clazz, entitySource);
//
//        return domainObject;
//    }


//    public static <T extends DomainObject> IteratorFindEntity<T> findAll(DataSource dataSource, Class<T> clazz, Map<String, Object> filters) throws ReflectiveOperationException, RocksDBException {
//        return new IteratorFindEntity(dataSource, clazz, filters);
//    }

    public static <T extends DomainObject> T createDomainObject(DataSource dataSource, final Class<T> clazz, EntitySource entitySource) throws ReflectiveOperationException, RocksDBException {
        Constructor<T> constructor = clazz.getConstructor(long.class);

        T domainObject = constructor.newInstance(entitySource.getId());


//        ProxyFactory factory = new ProxyFactory();
//        factory.setSuperclass(clazz);
//        factory.setFilter(getMethodFilter(clazz));
//
//        T domainObject = (T) factory.create(
//                new Class<?>[]{long.class},
//                new Object[]{entitySource.getId()},
//                getMethodHandler(clazz)
//        );

        //Устанавливаем dataSource
        HashStructEntities.getDataSourceField().set(domainObject, dataSource);

        //Загружаем поля
        Map<String, byte[]> data = entitySource.getFields();
        if (data!=null) {
            StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

//            for (String formatFieldName: structEntity.getFormatFieldNames()) {
//                Field field = HashStructEntities.getStructEntity(clazz).getFieldByFormatName(formatFieldName);
//                if (data.containsKey(formatFieldName)) {
//                    byte[] value = data.get(formatFieldName);
//                    field.set(domainObject, DomainObjectFieldValueUtils.unpackValue(domainObject, field, value));
//                } else {
//                    Field lazyLoadsField = HashStructEntities.getLazyLoadsField();
//
//                    Set<Field> lazyLoads = (Set<Field>) lazyLoadsField.get(domainObject);
//                    if (lazyLoads==null) {
//                        lazyLoads = new HashSet<Field>();
//                        lazyLoadsField.set(domainObject, lazyLoads);
//                    }
//                    lazyLoads.add(field);
//                }
//            }
        }

        return domainObject;
    }

//    public static <T extends DomainObject & DomainObjectEditable> void save(final Transaction transaction, final T domainObject) {
//
//        transaction.update(domainObject.getStructEntity(), domainObject, domainObject.getLoadValues(), domainObject.writeValues());
//        domainObject.flush();

//        //Смотрим какие поля не загружены и не отредактированы - такие не стоит перезаписывать
//        Set<String> updates = domainObject.waitWriteFields();
//
//        Set<Field> fields = new HashSet<>();
//        if (updates != null) {
//            for (String formatFieldName: structEntity.getFormatFieldNames()) {
//                Field field = structEntity.getFieldByFormatName(formatFieldName);
//                if (updates.contains(field)) fields.add(field);
//            }
//        }
//        transaction.update(structEntity, self, fields);
//
//
//
////        Field updatesField = HashStructEntities.getUpdatesField();
////        Set<Field> updates = (Set<Field>) updatesField.get(self);
//
//        Set<Field> fields = new HashSet<>();
//        if (updates != null) {
//            for (String formatFieldName: structEntity.getFormatFieldNames()) {
//                Field field = structEntity.getFieldByFormatName(formatFieldName);
//                if (updates.contains(field)) fields.add(field);
//            }
//        }
//        transaction.update(structEntity, self, fields);
//
//        //Сносим флаги, что поля были отредактированы
//        updatesField.set(self, null);
//    }



//    private static MethodFilter getMethodFilter(final Class<? extends DomainObject> clazz) {
//        MethodFilter methodFilter = methodFilters.get(clazz);
//        if (methodFilter==null) {
//            synchronized (methodFilters) {
//                methodFilter = methodFilters.get(clazz);
//                if (methodFilter==null) {
//                    methodFilter = new MethodFilterImpl(clazz);
//                    methodFilters.put(clazz, methodFilter);
//                }
//            }
//        }
//        return methodFilter;
//    }

//    private static MethodHandler getMethodHandler(final Class<? extends DomainObject> clazz) {
//        MethodHandler methodHandler = methodHandlers.get(clazz);
//        if (methodHandler==null) {
//            synchronized (methodFilters) {
//                methodHandler = methodHandlers.get(clazz);
//                if (methodHandler==null) {
//                    methodHandler = new MethodHandlerImpl(clazz);
//                    methodHandlers.put(clazz, methodHandler);
//                }
//            }
//        }
//        return methodHandler;
//    }
}
