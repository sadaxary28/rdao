package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.google.common.base.CaseFormat;
import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectFieldValueUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyAvailability;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyField;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import javassist.util.proxy.MethodHandler;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by kris on 29.12.16.
 */
public class MethodHandlerImpl implements MethodHandler {

    private final Class<? extends DomainObject> clazz;

    private final String columnFamily;

    public MethodHandlerImpl(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        this.columnFamily = entityAnnotation.columnFamily();
    }

    @Override
    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
        DomainObject domainObject = (DomainObject) self;
        StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

        if ("save".equals(thisMethod.getName())) {
            saveDomainObject(domainObject);
            return null;
        } else if ("remove".equals(thisMethod.getName())) {
            removeDomainObject(domainObject);
            return null;
        } else if (structEntity.isLazyGetterMethod(thisMethod.getName())) {
            Field field = structEntity.getFieldByLazyGetterMethod(thisMethod.getName());
            return getLazyValue(domainObject, field);
        } else {
            throw new RuntimeException("Not handler method: " + thisMethod.getName());
        }
    }

    private void saveDomainObject(DomainObject self) throws NoSuchFieldException, IllegalAccessException {
        Field transactionField = HashStructEntities.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null || !transaction.isActive()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");


        //TODO необходима оптимизация, в настоящий момент если поле не изменилось, мы все равно его перезаписываем
        Set<Field> fields = new HashSet<>();
        for (String formatFieldName: HashStructEntities.getStructEntity(clazz).getFormatFieldNames()) {
            Field field = HashStructEntities.getStructEntity(clazz).getField(formatFieldName);
            fields.add(field);
        }
        transaction.update(columnFamily, self, fields);
    }

    private void removeDomainObject(DomainObject self) throws NoSuchFieldException, IllegalAccessException {
        Field transactionField = HashStructEntities.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null || !transaction.isActive()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");

        throw new RuntimeException("Not implemented");
    }

    private Object getLazyValue(DomainObject domainObject, Field field) throws ReflectiveOperationException, RocksDBException {
        //Проверяем загружено ли это поле
        Field lazyLoadsField = HashStructEntities.getLazyLoadsField();
        Set<Field> lazyLoads = (Set<Field>) lazyLoadsField.get(domainObject);
        boolean isNeedLazyLoadingValue = (lazyLoads==null)?false:lazyLoads.contains(field);

        if (!isNeedLazyLoadingValue) {
            return field.get(domainObject);
        } else {
            //Требуется загрузка
            Entity entityAnnotation = clazz.getAnnotation(Entity.class);

            String formatFieldName = StructEntityUtils.getFormatFieldName(field);

            DataSource dataSource = (DataSource) HashStructEntities.getDataSourceField().get(domainObject);
            byte[] bValue = dataSource.getField(entityAnnotation.columnFamily(), domainObject.getId(), formatFieldName);
            Object value = DomainObjectFieldValueUtils.unpackValue(domainObject, field, bValue);

            field.set(domainObject, value);

            lazyLoads.remove(field);
            if (lazyLoads.isEmpty()) lazyLoadsField.set(domainObject, null);

            return value;
        }
    }
}
