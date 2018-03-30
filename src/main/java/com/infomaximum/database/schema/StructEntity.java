package com.infomaximum.database.schema;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.anotation.Index;
import com.infomaximum.database.anotation.IntervalIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.runtime.FieldNotFoundException;
import com.infomaximum.database.exception.runtime.StructEntityException;

import java.util.*;

public class StructEntity {

    public static class Reference {

        public final Class objClass;
        public final EntityIndex fieldIndex;

        private Reference(Class objClass, EntityIndex fieldIndex) {
            this.objClass = objClass;
            this.fieldIndex = fieldIndex;
        }
    }

    public final static String NAMESPACE_SEPARATOR = ".";

    private final Class<? extends DomainObject> clazz;
    private final String name;
    private final String columnFamily;
    private final Set<EntityField> fields;
    private final Map<String, EntityField> nameFields;
    private final List<EntityIndex> indexes;
    private final List<EntityPrefixIndex> prefixIndexes;
    private final List<EntityIntervalIndex> intervalIndexes;
    private final List<Reference> referencingForeignFields = new ArrayList<>();

    StructEntity(Class<? extends DomainObject> clazz) {
        final Entity annotationEntity = getAnnotationClass(clazz).getAnnotation(Entity.class);

        this.clazz = clazz;
        this.name = annotationEntity.name();
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
        this.intervalIndexes = buildIntervalIndexes(annotationEntity);
    }

    private void registerToForeignEntity(EntityField foreignField) {
        foreignField.getForeignDependency().referencingForeignFields.add(new Reference(clazz, buildForeignIndex(foreignField)));
    }

    public String getName() {
        return name;
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
        for (EntityIndex index : indexes) {
            if (index.sortedFields.size() != indexedFields.size()) {
                continue;
            }

            if (index.sortedFields.stream().allMatch(f -> indexedFields.contains(f.getName()))) {
                return index;
            }
        }
        return null;
    }

    public EntityPrefixIndex getPrefixIndex(Collection<String> indexedFields) {
        for (EntityPrefixIndex index : prefixIndexes) {
            if (index.sortedFields.size() != indexedFields.size()) {
                continue;
            }

            if (index.sortedFields.stream().allMatch(f -> indexedFields.contains(f.getName()))) {
                return index;
            }
        }
        return null;
    }

    public EntityIntervalIndex getIntervalIndex(Collection<String> hashedFields, String indexedField) {
        for (EntityIntervalIndex index : intervalIndexes) {
            if (index.sortedFields.size() != (hashedFields.size() + 1)) {
                continue;
            }

            if (!index.getIndexedField().getName().equals(indexedField)) {
                continue;
            }

            if (index.getHashedFields().stream().allMatch(f -> hashedFields.contains(f.getName()))) {
                return index;
            }
        }
        return null;
    }

    public List<EntityIndex> getIndexes() {
        return indexes;
    }

    public List<EntityPrefixIndex> getPrefixIndexes() {
        return prefixIndexes;
    }

    public List<EntityIntervalIndex> getIntervalIndexes() {
        return intervalIndexes;
    }

    public List<Reference> getReferencingForeignFields() {
        return referencingForeignFields;
    }

    static Class<? extends DomainObject> getAnnotationClass(Class<? extends DomainObject> clazz) {
        while (!clazz.isAnnotationPresent(Entity.class)) {
            if (!DomainObject.class.isAssignableFrom(clazz.getSuperclass())) {
                throw new StructEntityException("Not found " + Entity.class + " annotation in " + clazz + ".");
            }
            clazz = (Class<? extends DomainObject>) clazz.getSuperclass();
        }
        return clazz;
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
        for (Index index: entity.prefixIndexes()) {
            result.add(new EntityPrefixIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }

    private List<EntityIntervalIndex> buildIntervalIndexes(Entity entity) {
        List<EntityIntervalIndex> result = new ArrayList<>(entity.intervalIndexes().length);
        for (IntervalIndex index: entity.intervalIndexes()) {
            result.add(new EntityIntervalIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }
}

