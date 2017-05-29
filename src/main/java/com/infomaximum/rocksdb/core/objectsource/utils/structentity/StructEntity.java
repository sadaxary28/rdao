package com.infomaximum.rocksdb.core.objectsource.utils.structentity;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.anotation.Index;
import com.infomaximum.rocksdb.core.struct.DomainObject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    public final Class<? extends DomainObject> clazz;
    public final Entity annotationEntity;

    private final Map<String, Field> fieldsToNames;
    private final Map<String, Field> fieldsToFormatNames;
    private final Set<String> eagerFormatFieldNames;

    private final Map<Field, Method> lazyGetterFieldToMethods;
    private final Map<Method, Field> lazyGetterMethodToFields;

    public final Map<String, StructEntityIndex> indexs;

    public StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        this.annotationEntity = clazz.getAnnotation(Entity.class);
        if (annotationEntity==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        fieldsToNames = new HashMap<String, Field>();
        fieldsToFormatNames = new HashMap<String, Field>();

        Set<String> modifiableEagerFormatFieldNames = new HashSet<String>();
        eagerFormatFieldNames = Collections.unmodifiableSet(modifiableEagerFormatFieldNames);

        lazyGetterFieldToMethods = new HashMap<Field, Method>();
        lazyGetterMethodToFields = new HashMap<Method, Field>();

        //Читаем все поля
        for (Field field: clazz.getDeclaredFields()) {
            EntityField annotationEntityField = field.getAnnotation(EntityField.class);
            if (annotationEntityField==null) continue;

            //Обязательно проверяем что поле приватное
            if (!Modifier.isPrivate(field.getModifiers())) throw new RuntimeException("Field: " + field.getName() + " is not private");
            field.setAccessible(true);

            fieldsToNames.put(field.getName(), field);

            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
            fieldsToFormatNames.put(formatFieldName, field);

            if (!annotationEntityField.lazy()) {
                modifiableEagerFormatFieldNames.add(formatFieldName);
            }

            //Теперь ищем lazy getter'ы методы
            if (annotationEntityField.lazy()) {
                Method methodGetter = StructEntityUtils.findGetterMethod(clazz, field);
                if (methodGetter!=null) {
                    lazyGetterFieldToMethods.put(field, methodGetter);
                    lazyGetterMethodToFields.put(methodGetter, field);
                }
            }
        }

        Map<String, StructEntityIndex>  modifiableIndexs = new HashMap<>();
        for (Index index: annotationEntity.indexes()) {
            StructEntityIndex structEntityIndex = new StructEntityIndex(this, index);
            modifiableIndexs.put(structEntityIndex.name, structEntityIndex);
        }
        indexs = Collections.unmodifiableMap(modifiableIndexs);
    }

    public Set<String> getFormatFieldNames() {
        return fieldsToFormatNames.keySet();
    }

    public Set<String> getEagerFormatFieldNames(){
        return eagerFormatFieldNames;
    }

    public Field getFieldByName(String fieldName) {
        return fieldsToNames.get(fieldName);
    }

    public Field getFieldByFormatName(String formatFieldName) {
        return fieldsToFormatNames.get(formatFieldName);
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
