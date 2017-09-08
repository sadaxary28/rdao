package com.infomaximum.database.core.structentity;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;

import java.util.*;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    public final Class<? extends DomainObject> clazz;
    public final Entity annotationEntity;

    private final Set<Field> fields;
    private final Map<String, Field> nameToFields;

    private final Set<String> eagerFormatFieldNames;

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

        Set<String> modifiableEagerFormatFieldNames = new HashSet<String>();
        eagerFormatFieldNames = Collections.unmodifiableSet(modifiableEagerFormatFieldNames);

        indexs = new HashMap<>();
        for (Index index: annotationEntity.indexes()) {
            StructEntityIndex structEntityIndex = new StructEntityIndex(this, index);
            indexs.put(structEntityIndex.name, structEntityIndex);
        }
    }

    public Field getFieldByName(String name) {
        return nameToFields.get(name);
    }


    public Set<Field> getFields() {
        return fields;
    }

    public Set<String> getEagerFormatFieldNames(){
        return eagerFormatFieldNames;
    }


    public StructEntityIndex getStructEntityIndex(Collection<String> nameIndexFields) {
        String nameIndex = StructEntityIndex.buildNameIndex(nameIndexFields);
        return indexs.get(nameIndex);
    }

    public static Class<? extends DomainObject> getEntityClass(Class<? extends DomainObject> clazz){
        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) {
            if (DomainObject.class.isAssignableFrom(clazz.getSuperclass())) {
                return getEntityClass((Class<? extends DomainObject>) clazz.getSuperclass());
            } else {
                throw new StructEntityDatabaseException("Not found 'Entity' annotation in class: " + clazz);
            }
        } else {
            return clazz;
        }
    }

    public static Entity getEntityAnnotation(Class<? extends DomainObject> clazz) {
        return getEntityClass(clazz).getAnnotation(Entity.class);
    }
}

