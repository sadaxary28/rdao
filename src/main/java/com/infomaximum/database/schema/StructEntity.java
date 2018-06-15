package com.infomaximum.database.schema;

import com.infomaximum.database.anotation.*;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.runtime.FieldNotFoundException;
import com.infomaximum.database.exception.runtime.IndexNotFoundException;
import com.infomaximum.database.exception.runtime.StructEntityException;

import java.util.*;

public class StructEntity {

    public static class Reference {

        public final Class objClass;
        public final HashIndex fieldIndex;

        private Reference(Class objClass, HashIndex fieldIndex) {
            this.objClass = objClass;
            this.fieldIndex = fieldIndex;
        }
    }

    public final static String NAMESPACE_SEPARATOR = ".";

    private final Class<? extends DomainObject> clazz;
    private final String name;
    private final String columnFamily;
    private final Set<Field> fields;
    private final Map<String, Field> nameFields;
    private final List<HashIndex> hashIndexes;
    private final List<PrefixIndex> prefixIndexes;
    private final List<IntervalIndex> intervalIndexes;
    private final List<RangeIndex> rangeIndexes;
    private final List<Reference> referencingForeignFields = new ArrayList<>();

    StructEntity(Class<? extends DomainObject> clazz) {
        final Entity annotationEntity = getAnnotationClass(clazz).getAnnotation(Entity.class);

        this.clazz = clazz;
        this.name = annotationEntity.name();
        this.columnFamily = buildColumnFamily(annotationEntity);

        Set<Field> modifiableFields = new HashSet<>(annotationEntity.fields().length);
        Map<String, Field> modifiableNameToFields = new HashMap<>(annotationEntity.fields().length);

        for(com.infomaximum.database.anotation.Field field: annotationEntity.fields()) {
            //Проверяем на уникальность
            if (modifiableNameToFields.containsKey(field.name())) {
                throw new StructEntityException("Field " + field.name() + " already exists into " + clazz.getName() + ".");
            }

            Field f = new Field(field, this);

            modifiableFields.add(f);
            modifiableNameToFields.put(f.getName(), f);

            if (f.isForeign()) {
                registerToForeignEntity(f);
            }
        }

        this.nameFields = Collections.unmodifiableMap(modifiableNameToFields);
        this.fields = Collections.unmodifiableSet(modifiableFields);
        this.hashIndexes = buildHashIndexes(annotationEntity);
        this.prefixIndexes = buildPrefixIndexes(annotationEntity);
        this.intervalIndexes = buildIntervalIndexes(annotationEntity);
        this.rangeIndexes = buildRangeIndexes(annotationEntity);
    }

    private void registerToForeignEntity(Field foreignField) {
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

    public Field getField(String name) {
        Field field = nameFields.get(name);
        if (field == null) {
            throw new FieldNotFoundException(clazz, name);
        }
        return field;
    }

    public Set<Field> getFields() {
        return fields;
    }

    public HashIndex getHashIndex(Collection<String> indexedFields) {
        for (HashIndex index : hashIndexes) {
            if (index.sortedFields.size() != indexedFields.size()) {
                continue;
            }

            if (index.sortedFields.stream().allMatch(f -> indexedFields.contains(f.getName()))) {
                return index;
            }
        }

        throw new IndexNotFoundException(HashIndex.toString(indexedFields), clazz);
    }

    public PrefixIndex getPrefixIndex(Collection<String> indexedFields) {
        for (PrefixIndex index : prefixIndexes) {
            if (index.sortedFields.size() != indexedFields.size()) {
                continue;
            }

            if (index.sortedFields.stream().allMatch(f -> indexedFields.contains(f.getName()))) {
                return index;
            }
        }

        throw new IndexNotFoundException(PrefixIndex.toString(indexedFields), clazz);
    }

    public IntervalIndex getIntervalIndex(Collection<String> hashedFields, String indexedField) {
        for (IntervalIndex index : intervalIndexes) {
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

        throw new IndexNotFoundException(IntervalIndex.toString(hashedFields, indexedField), clazz);
    }

    public RangeIndex getRangeIndex(Collection<String> hashedFields, String beginField, String endField) {
        for (RangeIndex index : rangeIndexes) {
            if (index.sortedFields.size() != (hashedFields.size() + 2)) {
                continue;
            }

            if (!index.getBeginIndexedField().getName().equals(beginField) || !index.getEndIndexedField().getName().equals(endField)) {
                continue;
            }

            if (index.getHashedFields().stream().allMatch(f -> hashedFields.contains(f.getName()))) {
                return index;
            }
        }

        throw new IndexNotFoundException(RangeIndex.toString(hashedFields, beginField, endField), clazz);
    }

    public List<HashIndex> getHashIndexes() {
        return hashIndexes;
    }

    public List<PrefixIndex> getPrefixIndexes() {
        return prefixIndexes;
    }

    public List<IntervalIndex> getIntervalIndexes() {
        return intervalIndexes;
    }

    public List<RangeIndex> getRangeIndexes() {
        return rangeIndexes;
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

    private List<HashIndex> buildHashIndexes(Entity entity) {
        List<HashIndex> result = new ArrayList<>(entity.hashIndexes().length);
        for (com.infomaximum.database.anotation.HashIndex index: entity.hashIndexes()) {
            result.add(new HashIndex(index, this));
        }

        for (Field field : fields) {
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

    private HashIndex buildForeignIndex(Field foreignField) {
        return new HashIndex(foreignField, this);
    }

    private List<PrefixIndex> buildPrefixIndexes(Entity entity) {
        if (entity.prefixIndexes().length == 0) {
            return Collections.emptyList();
        }

        List<PrefixIndex> result = new ArrayList<>(entity.prefixIndexes().length);
        for (com.infomaximum.database.anotation.PrefixIndex index: entity.prefixIndexes()) {
            result.add(new PrefixIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }

    private List<IntervalIndex> buildIntervalIndexes(Entity entity) {
        if (entity.intervalIndexes().length == 0) {
            return Collections.emptyList();
        }

        List<IntervalIndex> result = new ArrayList<>(entity.intervalIndexes().length);
        for (com.infomaximum.database.anotation.IntervalIndex index: entity.intervalIndexes()) {
            result.add(new IntervalIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }

    private List<RangeIndex> buildRangeIndexes(Entity entity) {
        if (entity.rangeIndexes().length == 0) {
            return Collections.emptyList();
        }

        List<RangeIndex> result = new ArrayList<>(entity.rangeIndexes().length);
        for (com.infomaximum.database.anotation.RangeIndex index: entity.rangeIndexes()) {
            result.add(new RangeIndex(index, this));
        }

        return Collections.unmodifiableList(result);
    }
}

