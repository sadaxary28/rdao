package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectFieldValueUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import javassist.util.proxy.MethodHandler;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by kris on 29.12.16.
 */
public class MethodHandlerImpl implements MethodHandler {

    private final StructEntity structEntity;

    private final String columnFamily;

    public MethodHandlerImpl(Class<? extends DomainObject> clazz) {
        this.structEntity = HashStructEntities.getStructEntity(clazz);

        Entity annotationEntity = clazz.getAnnotation(Entity.class);
        this.columnFamily = annotationEntity.columnFamily();
    }

    @Override
    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
        DomainObject domainObject = (DomainObject) self;

        if ("save".equals(thisMethod.getName())) {
            saveDomainObject(domainObject);
            return null;
        } else if ("remove".equals(thisMethod.getName())) {
            removeDomainObject(domainObject);
            return null;
        } else if (structEntity.isGetterMethod(thisMethod.getName())) {
            Field field = structEntity.getFieldByGetterMethod(thisMethod.getName());
            return getLazyValue(domainObject, field);
        } else if (structEntity.isSetterMethod(thisMethod.getName())) {
            Field field = structEntity.getFieldBySetterMethod(thisMethod.getName());
            setValue(domainObject, field, args[0]);
            return null;
        } else {
            throw new RuntimeException("Not handler method: " + thisMethod.getName());
        }
    }

    private void saveDomainObject(DomainObject self) throws NoSuchFieldException, IllegalAccessException {
        Field transactionField = HashStructEntities.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null || !transaction.isActive()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");

        //Смотрим какие поля не загружены и не отредактированы - такие не стоит перезаписывать
        Field updatesField = HashStructEntities.getUpdatesField();
        Set<Field> updates = (Set<Field>) updatesField.get(self);

        Set<Field> fields = new HashSet<>();
        if (updates != null) {
            for (String formatFieldName: structEntity.getFormatFieldNames()) {
                Field field = structEntity.getFieldByFormatName(formatFieldName);
                if (updates.contains(field)) fields.add(field);
            }
        }
        transaction.update(structEntity, self, fields);

        //Сносим флаги, что поля были отредактированы
        updatesField.set(self, null);
    }

    private void removeDomainObject(DomainObject self) throws NoSuchFieldException, IllegalAccessException {
        Field transactionField = HashStructEntities.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null || !transaction.isActive()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");
        transaction.remove(columnFamily, self);
    }

    private Object getLazyValue(DomainObject self, Field field) throws ReflectiveOperationException, RocksDBException {
        //Проверяем загружено ли это поле
        Field lazyLoadsField = HashStructEntities.getLazyLoadsField();
        Set<Field> lazyLoads = (Set<Field>) lazyLoadsField.get(self);
        boolean isNeedLazyLoadingValue = (lazyLoads==null)?false:lazyLoads.contains(field);

        if (!isNeedLazyLoadingValue) {
            return field.get(self);
        } else {
            //Требуется загрузка
            String formatFieldName = StructEntityUtils.getFormatFieldName(field);

            DataSource dataSource = (DataSource) HashStructEntities.getDataSourceField().get(self);
            byte[] bValue = dataSource.getField(structEntity.annotationEntity.columnFamily(), self.getId(), formatFieldName);
            Object value = DomainObjectFieldValueUtils.unpackValue(self, field, bValue);

            field.set(self, value);

            lazyLoads.remove(field);
            if (lazyLoads.isEmpty()) lazyLoadsField.set(self, null);

            return value;
        }
    }


    private void setValue(DomainObject self, Field field, Object value) throws IllegalAccessException {
        Field transactionField = HashStructEntities.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null || !transaction.isActive()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");

        //Снимаем флаг, что поле не загружено
        Field lazyLoadsField = HashStructEntities.getLazyLoadsField();
        Set<Field> lazyLoads = (Set<Field>) lazyLoadsField.get(self);
        if (lazyLoads!=null) {
            lazyLoads.remove(field);
            if (lazyLoads.isEmpty()) lazyLoadsField.set(self, null);
        }

        //Устанавливаем флаг, что поле было изменено
        Field updatesField = HashStructEntities.getUpdatesField();
        Set<Field> updates = (Set<Field>) updatesField.get(self);
        if (updates==null) {
            updates = new HashSet<Field>();
            updatesField.set(self, updates);
        }
        updates.add(field);


        //Пишем значение
        field.set(self, value);
    }
}
