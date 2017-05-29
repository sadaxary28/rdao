package com.infomaximum.rocksdb.core.objectsource.utils.structentity;

import com.google.common.base.CaseFormat;
import com.infomaximum.rocksdb.core.struct.DomainObject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Created by user on 17.05.2017.
 */
public class StructEntityUtils {

    public static String getFormatFieldName(Field field) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
    }

    public static Method findGetterMethod(Class clazz, Field field) {
        Class type = field.getType();
        boolean isBooleanType = (type == boolean.class || type == boolean.class);

        String fieldName = field.getName();
        String methodName = (isBooleanType)?"is":"get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return clazz.getDeclaredMethod(methodName);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    public static Method findSetterMethod(Class<? extends DomainObject> clazz, Field field) {
        String fieldName = field.getName();
        String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
            return clazz.getDeclaredMethod(methodName, field.getType());
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
