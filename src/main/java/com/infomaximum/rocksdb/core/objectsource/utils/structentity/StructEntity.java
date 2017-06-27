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

    private final Map<Field, Method> setterFieldToMethods;
    private final Map<Method, Field> setterMethodToFields;

    private final Map<Field, Method> getterFieldToMethods;
    private final Map<Method, Field> getterMethodToFields;

    public final Map<String, StructEntityIndex> indexs;

    public StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        this.annotationEntity = clazz.getAnnotation(Entity.class);
        if (annotationEntity==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);

        fieldsToNames = new HashMap<String, Field>();
        fieldsToFormatNames = new HashMap<String, Field>();

        Set<String> modifiableEagerFormatFieldNames = new HashSet<String>();
        eagerFormatFieldNames = Collections.unmodifiableSet(modifiableEagerFormatFieldNames);

        setterFieldToMethods = new HashMap<Field, Method>();
        setterMethodToFields = new HashMap<Method, Field>();

        getterFieldToMethods = new HashMap<Field, Method>();
        getterMethodToFields = new HashMap<Method, Field>();

        //Читаем все поля
        for (Field field: clazz.getDeclaredFields()) {
            EntityField annotationEntityField = field.getAnnotation(EntityField.class);
            if (annotationEntityField==null) continue;

            //Обязательно проверяем что поле приватное
            if (!Modifier.isPrivate(field.getModifiers())) throw new RuntimeException("In class: " + clazz + " field: " + field.getName() + " is not private");

            //Проверяем, что поле имеет "правильное" наименование
            StructEntityUtils.validateField(clazz, field);

            field.setAccessible(true);

            fieldsToNames.put(field.getName(), field);

            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
            fieldsToFormatNames.put(formatFieldName, field);

            if (!annotationEntityField.lazy()) {
                modifiableEagerFormatFieldNames.add(formatFieldName);
            }


            //Теперь ищем setter'ы методы
            Method methodSetter = StructEntityUtils.findSetterMethod(clazz, field);
            if (methodSetter!=null) {
                setterFieldToMethods.put(field, methodSetter);
                setterMethodToFields.put(methodSetter, field);
            }

            //Теперь ищем getter'ы методы
            Method methodGetter = StructEntityUtils.findGetterMethod(clazz, field);
            if (methodGetter == null ) {
                if (annotationEntityField.lazy()) throw new RuntimeException("In class: " + clazz + " to field: " + field.getName() + " not found getter");
            } else {
                getterFieldToMethods.put(field, methodGetter);
                getterMethodToFields.put(methodGetter, field);
            }
        }

        indexs = new HashMap<>();
        for (Index index: annotationEntity.indexes()) {
            StructEntityIndex structEntityIndex = new StructEntityIndex(this, index);
            indexs.put(structEntityIndex.name, structEntityIndex);
        }
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

    public boolean isSetterMethod(String methodName) {
        for (Method method: setterMethodToFields.keySet()) {
            if (method.getName().equals(methodName)) return true;
        }
        return false;
    }

    public boolean isGetterMethod(String methodName) {
        for (Method method: getterMethodToFields.keySet()) {
            if (method.getName().equals(methodName)) return true;
        }
        return false;
    }

    public Field getFieldByGetterMethod(String methodName) {
        for (Map.Entry<Method, Field> entry: getterMethodToFields.entrySet()) {
            Method method = entry.getKey();
            if (method.getName().equals(methodName)) return entry.getValue();
        }
        return null;
    }

    public Field getFieldBySetterMethod(String methodName) {
        for (Map.Entry<Method, Field> entry: setterMethodToFields.entrySet()) {
            Method method = entry.getKey();
            if (method.getName().equals(methodName)) return entry.getValue();
        }
        return null;
    }

    public Method getGetterMethodByField(Field field) {
        return getterFieldToMethods.get(field);
    }

    public StructEntityIndex getStructEntityIndex(Collection<String> nameIndexFields) {
        String nameIndex = StructEntityIndex.buildNameIndex(nameIndexFields);
        return indexs.get(nameIndex);
    }
}
