package com.infomaximum.rocksdb.core.objectsource.utils.structentity;

import com.google.common.base.CaseFormat;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    private final Class<? extends DomainObject> clazz;

    private final Map<String, Field> fields;
    private final Set<String> eagerFieldNames;

    private final Map<Field, Method> lazyGetterFieldToMethods;
    private final Map<Method, Field> lazyGetterMethodToFields;

    public StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        fields = new HashMap<String, Field>();

        Set<String> modifiableEagerFieldNames = new HashSet<String>();
        eagerFieldNames = Collections.unmodifiableSet(modifiableEagerFieldNames);

        lazyGetterFieldToMethods = new HashMap<Field, Method>();
        lazyGetterMethodToFields = new HashMap<Method, Field>();

        //Читаем все поля
        for (Field field: clazz.getDeclaredFields()) {
            EntityField entityField = field.getAnnotation(EntityField.class);
            if (entityField==null) continue;

            //Обязательно проверяем что поле приватное
            if (!Modifier.isPrivate(field.getModifiers())) throw new RuntimeException("Field: " + entityField.name() + " is not private");

            String fieldName = entityField.name();
            if (fieldName.isEmpty()) fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());

            fields.put(fieldName, field);
            field.setAccessible(true);

            if (!entityField.lazy()) {
                modifiableEagerFieldNames.add(fieldName);
            }


            //Теперь ищем lazy getter'ы методы
            if (entityField.lazy()) {
                Method methodGetter = findGetterMethod(clazz, field);
                if (methodGetter!=null) {
                    lazyGetterFieldToMethods.put(field, methodGetter);
                    lazyGetterMethodToFields.put(methodGetter, field);
                }
            }
        }

    }

    public Set<String> getFieldNames() {
        return fields.keySet();
    }

    public Set<String> getEagerFieldNames(){
        return eagerFieldNames;
    }

    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    public boolean isLazyGetterMethod(String methodName) {
        for (Method method: lazyGetterMethodToFields.keySet()) {
            if (method.getName().equals(methodName)) return true;
        }
        return false;
    }

    public Field getFieldByLazyGetterMethod(String methodName) {
        for (Map.Entry<Method, Field> entry: lazyGetterMethodToFields.entrySet()) {
            Method method = entry.getKey();
            if (method.getName().equals(methodName)) return entry.getValue();
        }
        return null;
    }


    private static Method findGetterMethod(Class clazz, Field field) {
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
}
