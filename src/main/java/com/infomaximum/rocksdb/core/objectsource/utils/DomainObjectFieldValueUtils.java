package com.infomaximum.rocksdb.core.objectsource.utils;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;

/**
 * Created by kris on 28.04.17.
 */
public class DomainObjectFieldValueUtils {

    public static byte[] packValue(DomainObject self, Field field) throws IllegalAccessException {
        Class type = field.getType();
        Object value = field.get(self);
        if (value == null) return null;

        if (DomainObject.class.isAssignableFrom(type)) {
            return TypeConvertRocksdb.pack(((DomainObject)value).getId());
        } else {
            return TypeConvertRocksdb.packObject(type, value);
        }
    }

    public static Object unpackValue(DomainObject self, Field field, byte[] value) throws ReflectiveOperationException, RocksDBException {
        Class type = field.getType();
        if (value == null) return null;

        if (DomainObject.class.isAssignableFrom(type)) {
            long id = TypeConvertRocksdb.getLong(value);

            Field transactionField = HashStructEntities.getTransactionField();
            Transaction transaction = (Transaction) transactionField.get(self);
            if (transaction!=null && !transaction.isActive()) transaction = null;

            Field dataSourceField = HashStructEntities.getDataSourceField();
            DataSource dataSource = (DataSource) dataSourceField.get(self);

            if (transaction==null) {
                return DomainObjectUtils.get(dataSource, type, id);
            } else {
                return DomainObjectUtils.edit(dataSource, transaction, type, id);
            }
        } else {
            return TypeConvertRocksdb.get(field.getType(), value);
        }
    }
}
