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
    private final Set<String> eagerFormatFieldNames;

    private final Map<Field, Method> lazyGetterFieldToMethods;
    private final Map<Method, Field> lazyGetterMethodToFields;

    public StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        fields = new HashMap<String, Field>();

        Set<String> modifiableEagerFormatFieldNames = new HashSet<String>();
        eagerFormatFieldNames = Collections.unmodifiableSet(modifiableEagerFormatFieldNames);

        lazyGetterFieldToMethods = new HashMap<Field, Method>();
        lazyGetterMethodToFields = new HashMap<Method, Field>();

        //Читаем все поля
        for (Field field: clazz.getDeclaredFields()) {
            EntityField entityField = field.getAnnotation(EntityField.class);
            if (entityField==null) continue;

            //Обязательно проверяем что поле приватное
            if (!Modifier.isPrivate(field.getModifiers())) throw new RuntimeException("Field: " + field.getName() + " is not private");

            String formatFieldName = StructEntityUtils.getFormatFieldName(field);

            fields.put(formatFieldName, field);
            field.setAccessible(true);

            if (!entityField.lazy()) {
                modifiableEagerFormatFieldNames.add(formatFieldName);
            }

            //Теперь ищем lazy getter'ы методы
            if (entityField.lazy()) {
                Method methodGetter = StructEntityUtils.findGetterMethod(clazz, field);
                if (methodGetter!=null) {
                    lazyGetterFieldToMethods.put(field, methodGetter);
                    lazyGetterMethodToFields.put(methodGetter, field);
                }
            }
        }

    }

    public Set<String> getFormatFieldNames() {
        return fields.keySet();
    }

    public Set<String> getEagerFormatFieldNames(){
        return eagerFormatFieldNames;
    }

    public Field getField(String formatFieldName) {
        return fields.get(formatFieldName);
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

}
