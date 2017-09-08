package com.infomaximum.database.utils;

import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;

/**
 * Created by kris on 28.04.17.
 */
public class DomainObjectFieldValueUtils {

//    public static byte[] packValue(DomainObject self, Field field) throws IllegalAccessException {
//        Class type = field.getType();
//        Object value = field.get(self);
//        if (value == null) return null;
//
//        if (DomainObjectOLD.class.isAssignableFrom(type)) {
//            return TypeConvertRocksdb.pack(((DomainObjectOLD)value).getId());
//        } else {
//            return TypeConvertRocksdb.packObject(type, value);
//        }
//    }

    public static Object unpackValue(DomainObject self, Field field, byte[] value) throws ReflectiveOperationException, RocksDBException {
        Class type = field.getType();
        if (value == null) return null;
        return TypeConvert.get(field.getType(), value);
    }

    public static <T> T unpackValue(DataSource dataSource, Class<T> type, byte[] value) throws ReflectiveOperationException, RocksDBException {
        if (value == null) return null;
        return (T) TypeConvert.get(type, value);
    }
}
