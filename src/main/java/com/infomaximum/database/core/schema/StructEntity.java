package com.infomaximum.database.core.schema;

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

    private final Class<? extends DomainObject> clazz;
    private final String name;
    private final Set<EntityField> fields;
    private final Map<String, EntityField> nameFields;
    private final List<EntityIndex> indices;

    private StructEntity(Class<? extends DomainObject> clazz) {
        final Entity annotationEntity = getAnnotationClass(clazz).getAnnotation(Entity.class);

        this.clazz = clazz;
        this.name = annotationEntity.name();

        Set<EntityField> modifiableFields = new HashSet<>(annotationEntity.fields().length);
        Map<String, EntityField> modifiableNameToFields = new HashMap<>(annotationEntity.fields().length);

        for(Field field: annotationEntity.fields()) {
            //Проверяем на уникальность
            if (modifiableNameToFields.containsKey(field.name())) {
                throw new StructEntityDatabaseException("Field " + field.name() + " already exists into " + clazz.getName());
            }

            EntityField f = new EntityField(field);

            modifiableFields.add(f);
            modifiableNameToFields.put(f.getName(), f);
        }

        this.nameFields = Collections.unmodifiableMap(modifiableNameToFields);
        this.fields = Collections.unmodifiableSet(modifiableFields);

        List<EntityIndex> modifiableStructEntityIndices = new ArrayList<>(annotationEntity.indexes().length);
        for (Index index: annotationEntity.indexes()) {
            modifiableStructEntityIndices.add(new EntityIndex(index, this));
        }

        this.indices = Collections.unmodifiableList(modifiableStructEntityIndices);
    }

    public String getName() {
        return name;
    }

    public EntityField getField(String name) {
        EntityField field = nameFields.get(name);
        if (field == null) {
            throw new FieldNotFoundDatabaseException(clazz, name);
        }
        return field;
    }

    public Set<EntityField> getFields() {
        return fields;
    }

    public EntityIndex getStructEntityIndex(Collection<String> nameIndexFields) {
        for (EntityIndex entityIndex : indices) {
            if (entityIndex.sortedFields.size() != nameIndexFields.size()) {
                continue;
            }

            List<String> iNameIndexFields = entityIndex.sortedFields.stream().map(EntityField::getName).collect(Collectors.toList());

            if (!iNameIndexFields.containsAll(nameIndexFields)) continue;
            if (!nameIndexFields.containsAll(iNameIndexFields)) continue;

            return entityIndex;
        }
        return null;
    }

    public List<EntityIndex> getIndices() {
        return indices;
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

