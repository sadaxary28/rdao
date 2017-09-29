package com.infomaximum.database.core.structentity;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.runtime.FieldNotFoundDatabaseException;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    private final static Logger log = LoggerFactory.getLogger(StructEntity.class);

    public final static java.lang.reflect.Field dataSourceField = getDataSourceField();
    private final static ConcurrentMap<Class<? extends DomainObject>, StructEntity> structEntities = new ConcurrentHashMap<>();

    public final Entity annotationEntity;

    private final Class<? extends DomainObject> clazz;
    private final Set<Field> fields;
    private final Map<String, Field> nameToFields;
    private final List<StructEntityIndex> structEntityIndices;

    private StructEntity(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;
        this.annotationEntity = getAnnotationClass(clazz).getAnnotation(Entity.class);

        Set<Field> modifiableFields = new HashSet<>(annotationEntity.fields().length);
        Map<String, Field> modifiableNameToFields = new HashMap<>(annotationEntity.fields().length);

        for(Field field: annotationEntity.fields()) {
            //Проверяем на уникальность
            if (modifiableNameToFields.containsKey(field.name())) {
                throw new StructEntityDatabaseException("Поле " + field.name() + " уже объявлено в " + clazz.getName());
            }

            modifiableFields.add(field);
            modifiableNameToFields.put(field.name(), field);
        }

        this.nameToFields = Collections.unmodifiableMap(modifiableNameToFields);
        this.fields = Collections.unmodifiableSet(modifiableFields);

        List<StructEntityIndex> modifiableStructEntityIndices = new ArrayList<>(annotationEntity.indexes().length);
        for (Index index: annotationEntity.indexes()) {
            modifiableStructEntityIndices.add(new StructEntityIndex(this, index));
        }

        this.structEntityIndices = Collections.unmodifiableList(modifiableStructEntityIndices);
    }

    public Field getFieldByName(String name) {
        Field field = nameToFields.get(name);
        if (field == null) {
            throw new FieldNotFoundDatabaseException(clazz, name);
        }
        return field;
    }

    public Set<Field> getFields() {
        return fields;
    }

    public StructEntityIndex getStructEntityIndex(Collection<String> nameIndexFields) {
        for (StructEntityIndex structEntityIndex: structEntityIndices) {
            if (structEntityIndex.sortedFields.size() != nameIndexFields.size()) {
                continue;
            }

            List<String> iNameIndexFields = structEntityIndex.sortedFields.stream().map(Field::name).collect(Collectors.toList());

            if (!iNameIndexFields.containsAll(nameIndexFields)) continue;
            if (!nameIndexFields.containsAll(iNameIndexFields)) continue;

            return structEntityIndex;
        }
        return null;
    }

    public List<StructEntityIndex> getStructEntityIndices() {
        return structEntityIndices;
    }

    public static StructEntity getInstance(Class<? extends DomainObject> clazz) {
        Class<? extends DomainObject> entityClass = StructEntity.getAnnotationClass(clazz);

        StructEntity domainObjectFields = structEntities.get(entityClass);
        if (domainObjectFields != null) {
            return domainObjectFields;
        }

        StructEntity newValue = new StructEntity(entityClass);
        domainObjectFields = structEntities.putIfAbsent(entityClass, newValue);
        return domainObjectFields != null ? domainObjectFields : newValue;
    }

    private static java.lang.reflect.Field getDataSourceField() {
        java.lang.reflect.Field field = null;
        try {
            field = DomainObject.class.getDeclaredField("dataSource");
            field.setAccessible(true);
        } catch (Exception e) {
            log.error("Exception StructEntity.getDataSourceField", e);
        }

        return field;
    }

    private static Class<? extends DomainObject> getAnnotationClass(Class<? extends DomainObject> clazz) {
        while (true) {
            if (clazz.isAnnotationPresent(Entity.class)) {
                return clazz;
            }
            if (!DomainObject.class.isAssignableFrom(clazz.getSuperclass())) {
                throw new StructEntityDatabaseException("Not found " + Entity.class + " annotation in " + clazz);
            }
            clazz = (Class<? extends DomainObject>) clazz.getSuperclass();
        }
    }
}

