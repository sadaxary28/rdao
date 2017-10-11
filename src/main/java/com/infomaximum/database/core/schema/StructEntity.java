package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.core.anotation.PrefixIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.runtime.FieldNotFoundException;
import com.infomaximum.database.exeption.runtime.StructEntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by kris on 26.04.17.
 */
public class StructEntity {

    private final static Logger log = LoggerFactory.getLogger(StructEntity.class);

    public static class Reference {

        public final Class objClass;
        public final EntityIndex fieldIndex;

        private Reference(Class objClass, EntityIndex fieldIndex) {
            this.objClass = objClass;
            this.fieldIndex = fieldIndex;
        }
    }

    public final static String NAMESPACE_SEPARATOR = ".";
    public final static java.lang.reflect.Field dataSourceField = getDataSourceField();

    private final Class<? extends DomainObject> clazz;
    private final String columnFamily;
    private final Set<EntityField> fields;
    private final Map<String, EntityField> nameFields;
    private final List<EntityIndex> indexes;
    private final List<EntityPrefixIndex> prefixIndexes;
    private final List<Reference> referencingForeignFields = new ArrayList<>();

    StructEntity(Class<? extends DomainObject> clazz) {
        final Entity annotationEntity = getAnnotationClass(clazz).getAnnotation(Entity.class);

        this.clazz = clazz;
        this.columnFamily = buildColumnFamily(annotationEntity);

        Set<EntityField> modifiableFields = new HashSet<>(annotationEntity.fields().length);
        Map<String, EntityField> modifiableNameToFields = new HashMap<>(annotationEntity.fields().length);

        for(Field field: annotationEntity.fields()) {
            //Проверяем на уникальность
            if (modifiableNameToFields.containsKey(field.name())) {
                throw new StructEntityException("Field " + field.name() + " already exists into " + clazz.getName() + ".");
            }

            EntityField f = new EntityField(field, this);

            modifiableFields.add(f);
            modifiableNameToFields.put(f.getName(), f);

            if (f.isForeign()) {
                registerToForeignEntity(f);
            }
        }

        this.nameFields = Collections.unmodifiableMap(modifiableNameToFields);
        this.fields = Collections.unmodifiableSet(modifiableFields);
        this.indexes = buildIndexes(annotationEntity);
        this.prefixIndexes = buildPrefixIndexes(annotationEntity);
    }

    private void registerToForeignEntity(EntityField foreignField) {
        foreignField.getForeignDependency().referencingForeignFields.add(new Reference(clazz, buildForeignIndex(foreignField)));
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public Class<? extends DomainObject> getObjectClass() {
        return clazz;
    }

    public EntityField getField(String name) {
        EntityField field = nameFields.get(name);
        if (field == null) {
            throw new FieldNotFoundException(clazz, name);
        }
        return field;
    }

    public Set<EntityField> getFields() {
        return fields;
    }

    public EntityIndex getIndex(Collection<String> indexedFields) {
        for (EntityIndex entityIndex : indexes) {
            if (entityIndex.sortedFields.size() != indexedFields.size()) {
                continue;
            }

            List<String> iNameIndexFields = entityIndex.sortedFields.stream().map(EntityField::getName).collect(Collectors.toList());

            if (!iNameIndexFields.containsAll(indexedFields)) continue;
            if (!indexedFields.containsAll(iNameIndexFields)) continue;

            return entityIndex;
        }
        return null;
    }

    public List<EntityIndex> getIndexes() {
        return indexes;
    }

    public List<EntityPrefixIndex> getPrefixIndexes() {
        return prefixIndexes;
    }

    public List<Reference> getReferencingForeignFields() {
        return referencingForeignFields;
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

    public static Class<? extends DomainObject> getAnnotationClass(Class<? extends DomainObject> clazz) {
        while (true) {
            if (clazz.isAnnotationPresent(Entity.class)) {
                return clazz;
            }
            if (!DomainObject.class.isAssignableFrom(clazz.getSuperclass())) {
                throw new StructEntityException("Not found " + Entity.class + " annotation in " + clazz + ".");
            }
            clazz = (Class<? extends DomainObject>) clazz.getSuperclass();
        }
    }

    private static String buildColumnFamily(Entity entity) {
        return new StringBuilder(entity.namespace())
                .append(NAMESPACE_SEPARATOR)
                .append(entity.name())
                .toString();
    }

    private List<EntityIndex> buildIndexes(Entity entity) {
        List<EntityIndex> result = new ArrayList<>(entity.indexes().length);
        for (Index index: entity.indexes()) {
            result.add(new EntityIndex(index, this));
        }

        for (EntityField field : fields) {
            if (!field.isForeign()) {
                continue;
            }

            if (result.stream().anyMatch(index -> index.sortedFields.size() == 1 && index.sortedFields.get(0) == field)) {
                continue;
            }

            result.add(buildForeignIndex(field));
        }

        return Collections.unmodifiableList(result);
    }

    private EntityIndex buildForeignIndex(EntityField foreignField) {
        return new EntityIndex(foreignField, this);
    }

    private List<EntityPrefixIndex> buildPrefixIndexes(Entity entity) {
        List<EntityPrefixIndex> result = new ArrayList<>(entity.prefixIndexes().length);
        for (PrefixIndex index: entity.prefixIndexes()) {
            result.add(new EntityPrefixIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }
}

