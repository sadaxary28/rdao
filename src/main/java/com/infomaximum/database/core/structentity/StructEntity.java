package com.infomaximum.database.core.structentity;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.struct.StructEntityDatabaseException;

import java.util.*;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    public final Class<? extends DomainObject> clazz;
    public final Entity annotationEntity;

    private final Set<Field> fields;
    private final Map<String, Field> nameToFields;


//    private final Map<String, java.lang.reflect.Field> fieldsToNames;
//    private final Map<String, java.lang.reflect.Field> fieldsToFormatNames;
    private final Set<String> eagerFormatFieldNames;

//    private final Map<java.lang.reflect.Field, Method> setterFieldToMethods;
//    private final Map<Method, java.lang.reflect.Field> setterMethodToFields;
//
//    private final Map<java.lang.reflect.Field, Method> getterFieldToMethods;
//    private final Map<Method, java.lang.reflect.Field> getterMethodToFields;

    public final Map<String, StructEntityIndex> indexs;

    public StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;

        this.annotationEntity = StructEntity.getEntityAnnotation(clazz);

        Set<Field> modifiableFields = new HashSet<Field>();
        this.fields = Collections.unmodifiableSet(modifiableFields);

        Map<String, Field> modifiableNameToFields = new HashMap<String, Field>();
        this.nameToFields = Collections.unmodifiableMap(modifiableNameToFields);

        for(Field field: annotationEntity.fields()) {
            //Проверяем на уникальность
            if (nameToFields.containsKey(field.name())) throw new StructEntityDatabaseException(clazz.getName() + ": Имя поля " + field.name() + " не уникально");

            modifiableFields.add(field);
            modifiableNameToFields.put(field.name(), field);
        }





//        fieldsToNames = new HashMap<String, java.lang.reflect.Field>();
//        fieldsToFormatNames = new HashMap<String, java.lang.reflect.Field>();

        Set<String> modifiableEagerFormatFieldNames = new HashSet<String>();
        eagerFormatFieldNames = Collections.unmodifiableSet(modifiableEagerFormatFieldNames);

//        setterFieldToMethods = new HashMap<java.lang.reflect.Field, Method>();
//        setterMethodToFields = new HashMap<Method, java.lang.reflect.Field>();
//
//        getterFieldToMethods = new HashMap<java.lang.reflect.Field, Method>();
//        getterMethodToFields = new HashMap<Method, java.lang.reflect.Field>();

        //Ищем поля
//        for (java.lang.reflect.Field field: clazz.getDeclaredFields()) {
//            Field annotationField = field.getAnnotation(Field.class);
//            if (annotationField ==null) continue;
//
//            //Проверяем, что тип String
//            if (field.getType()!=String.class) throw new StructEntityDatabaseException("Class: " + clazz + ", field: " + field);
//
//            //Проверяем, что final
//            if (!Modifier.isFinal(field.getModifiers())) throw new StructEntityDatabaseException("Class: " + clazz + ", field: " + field);
//
//            //Проверяем, что static
//            if (!Modifier.isStatic(field.getModifiers())) throw new StructEntityDatabaseException("Class: " + clazz + ", field: " + field);

            //Проверяем на дубликаты
//            annotationField
//
//            modifiableFields.add(field);
//        }

        //Читаем все поля
        for (java.lang.reflect.Field field: clazz.getDeclaredFields()) {
            Field annotationField = field.getAnnotation(Field.class);
            if (annotationField ==null) continue;

            //Обязательно проверяем что поле приватное
//            if (!Modifier.isPrivate(field.getModifiers())) throw new RuntimeException("In class: " + clazz + " field: " + field.getName() + " is not private");

            //Проверяем, что поле имеет "правильное" наименование
//            StructEntityUtils.validateField(clazz, field);
//
//            field.setAccessible(true);

//            modifiableFields.add(field);

//            fieldsToNames.put(field.getName(), field);
//
//            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
//            fieldsToFormatNames.put(formatFieldName, field);
//
//            if (!annotationEntityField.lazy()) {
//                modifiableEagerFormatFieldNames.add(formatFieldName);
//            }


//            //Теперь ищем setter'ы методы
//            Method methodSetter = StructEntityUtils.findSetterMethod(clazz, field);
//            if (methodSetter!=null) {
//                setterFieldToMethods.put(field, methodSetter);
//                setterMethodToFields.put(methodSetter, field);
//            }
//
//            //Теперь ищем getter'ы методы
//            Method methodGetter = StructEntityUtils.findGetterMethod(clazz, field);
//            if (methodGetter == null ) {
//                if (annotationEntityField.lazy()) throw new RuntimeException("In class: " + clazz + " to field: " + field.getName() + " not found getter");
//            } else {
//                getterFieldToMethods.put(field, methodGetter);
//                getterMethodToFields.put(methodGetter, field);
//            }
        }

        indexs = new HashMap<>();
        for (Index index: annotationEntity.indexes()) {
            StructEntityIndex structEntityIndex = new StructEntityIndex(this, index);
            indexs.put(structEntityIndex.name, structEntityIndex);
        }
    }

    public Field getFieldByName(String name) {
        return nameToFields.get(name);
    }

//    public Set<Field> getFields() {
//        return fields;
//    }

    public Set<Field> getFields() {
        return fields;
    }

    public Set<String> getEagerFormatFieldNames(){
        return eagerFormatFieldNames;
    }

//    public java.lang.reflect.Field getFieldByName(String fieldName) {
//        return fieldsToNames.get(fieldName);
//    }

//    public java.lang.reflect.Field getFieldByFormatName(String formatFieldName) {
//        return fieldsToFormatNames.get(formatFieldName);
//    }

//    public boolean isSetterMethod(String methodName) {
//        for (Method method: setterMethodToFields.keySet()) {
//            if (method.getName().equals(methodName)) return true;
//        }
//        return false;
//    }
//
//    public boolean isGetterMethod(String methodName) {
//        for (Method method: getterMethodToFields.keySet()) {
//            if (method.getName().equals(methodName)) return true;
//        }
//        return false;
//    }

//    public java.lang.reflect.Field getFieldByGetterMethod(String methodName) {
//        for (Map.Entry<Method, java.lang.reflect.Field> entry: getterMethodToFields.entrySet()) {
//            Method method = entry.getKey();
//            if (method.getName().equals(methodName)) return entry.getValue();
//        }
//        return null;
//    }
//
//    public java.lang.reflect.Field getFieldBySetterMethod(String methodName) {
//        for (Map.Entry<Method, java.lang.reflect.Field> entry: setterMethodToFields.entrySet()) {
//            Method method = entry.getKey();
//            if (method.getName().equals(methodName)) return entry.getValue();
//        }
//        return null;
//    }

//    public Method getGetterMethodByField(java.lang.reflect.Field field) {
//        return getterFieldToMethods.get(field);
//    }

    public StructEntityIndex getStructEntityIndex(Collection<String> nameIndexFields) {
        String nameIndex = StructEntityIndex.buildNameIndex(nameIndexFields);
        return indexs.get(nameIndex);
    }

    public static Class<? extends DomainObject> getEntityClass(Class<? extends DomainObject> clazz) {
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) {
            if (DomainObject.class.isAssignableFrom(clazz.getSuperclass())) {
                return getEntityClass((Class<? extends DomainObject>) clazz.getSuperclass());
            } else {
                throw new DatabaseException("Not found 'Entity' annotation in class: " + clazz);
            }
        } else {
            return clazz;
        }
    }

    public static Entity getEntityAnnotation(Class<? extends DomainObject> clazz) {
        return getEntityClass(clazz).getAnnotation(Entity.class);
    }
}

