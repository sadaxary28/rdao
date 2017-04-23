package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.google.common.base.CaseFormat;
import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.HashFields;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import javassist.util.proxy.MethodHandler;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

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
        if ("save".equals(thisMethod.getName())) {
            saveDomainObject((DomainObject) self);
            return null;
        } else {
            throw new RuntimeException("Not handler method: " + thisMethod.getName());
        }
    }

    private void saveDomainObject(DomainObject self) throws NoSuchFieldException, IllegalAccessException {
        Field transactionField = HashFields.getTransactionField();
        Transaction transaction = (Transaction) transactionField.get(self);
        if (transaction==null) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");

        //TODO реализовать, что бы небыло перезаписы
        for (String fieldName: HashFields.getEntityFieldNames(clazz)) {
            Field field = HashFields.getEntityField(clazz, fieldName);

            String key = DomainObjectUtils.getRocksDBKey(self.getId(), fieldName);
            byte[] value = TypeConvertRocksdb.packObject(field.getType(), field.get(self));
            transaction.put(columnFamily, key, value);
        }
    }
}
