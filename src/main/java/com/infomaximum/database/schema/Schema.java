package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.exception.TableNotFoundException;
import com.infomaximum.database.provider.DBProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class Schema {

    public static class Builder {

        private final Set<Class<? extends DomainObject>> domainClasses = new HashSet<>();

        public Builder withDomain(Class<? extends DomainObject> clazz) {
            if (!domainClasses.add(clazz)) {
                throw new RuntimeException("Class " + clazz + " already exists.");
            }
            return this;
        }

        public Schema build(DBProvider dbProvider) {
            return new Schema(domainClasses, dbProvider);
        }
    }

    private final static ConcurrentMap<Class<? extends DomainObject>, StructEntity> entities = new ConcurrentHashMap<>();

    private final Set<StructEntity> domains;
    private final com.infomaximum.database.schema.newschema.Schema dbSchema;

    private Schema(Set<Class<? extends DomainObject>> domainClasses, DBProvider dbProvider) {
        try {
            this.dbSchema = com.infomaximum.database.schema.newschema.Schema.read(dbProvider);
        } catch (DatabaseException e) {
            throw new SchemaException(e);
        }
        Set<StructEntity> modifiableDomains = new HashSet<>(domainClasses.size());
        for (Class<? extends DomainObject> domain : domainClasses) {
            modifiableDomains.add(ensureEntity(domain));
        }

        this.domains = Collections.unmodifiableSet(modifiableDomains);
    }

    public Set<StructEntity> getDomains() {
        return domains;
    }

    public boolean isEmpty() {
        return domains.isEmpty();
    }

    public static StructEntity getEntity(Class<? extends DomainObject> clazz) {
        StructEntity entity = entities.get(clazz);
        if (entity == null) {
            entity = entities.get(StructEntity.getAnnotationClass(clazz));
            entities.putIfAbsent(clazz, entity);
        }
        return entity;
    }

    public static void install(Set<Class<? extends DomainObject>> domainClasses, DBProvider dbProvider) throws DatabaseException {
        com.infomaximum.database.schema.newschema.Schema schema;
        if (com.infomaximum.database.schema.newschema.Schema.exists(dbProvider)) {
            schema = com.infomaximum.database.schema.newschema.Schema.read(dbProvider);
        } else {
            schema = com.infomaximum.database.schema.newschema.Schema.create(dbProvider);
        }
        Set<StructEntity> modifiableDomains = domainClasses.stream().map(Schema::getNewEntity).collect(Collectors.toSet());
        if (modifiableDomains.stream().noneMatch(schema::existTable)) {
            createTables(schema, modifiableDomains);
        }
        if (modifiableDomains.stream().anyMatch(d -> !schema.existTable(d))) {
            throw new SchemaException("Inconsistent schema error. Schema doesn't contain table: " + modifiableDomains.stream().filter(d -> !schema.existTable(d))
                    .map(StructEntity::getColumnFamily)
                    .findAny()
                    .orElse(null));
        }
    }

    private static void createTables(com.infomaximum.database.schema.newschema.Schema schema, Set<StructEntity> modifiableDomains) throws DatabaseException {
        Set<StructEntity> notCreatedTables = new HashSet<>();
        for (StructEntity domain : modifiableDomains) {
            if (isForeignDependenciesCreated(schema, domain)) {
                schema.createTable(domain);
            } else {
                notCreatedTables.add(domain);
            }
        }
        if (notCreatedTables.size() == 0) {
            return;
        }
        if (notCreatedTables.size() == modifiableDomains.size()) {
            throw new TableNotFoundException(notCreatedTables.stream().findFirst().get().getName());
        }
        createTables(schema, notCreatedTables);
    }

    private static boolean isForeignDependenciesCreated(com.infomaximum.database.schema.newschema.Schema schema, StructEntity domain) {
        for (Field field : domain.getFields()) {
            if (field.isForeign() && field.getForeignDependency() != domain && !schema.existTable(field.getForeignDependency())) {
                return false;
            }
        }
        return true;
    }

    public com.infomaximum.database.schema.newschema.Schema getDbSchema() {
        return dbSchema;
    }

    static StructEntity ensureEntity(Class<? extends DomainObject> domain) {
        Class<? extends DomainObject> annotationClass = StructEntity.getAnnotationClass(domain);
        StructEntity entity = entities.get(annotationClass);
        if (entity == null) {
            entity = new StructEntity(annotationClass);
            entities.put(annotationClass, entity);
        }
        return entity;
    }

    private static StructEntity getNewEntity(Class<? extends DomainObject> domain) {
        Class<? extends DomainObject> annotationClass = StructEntity.getAnnotationClass(domain);
        return new StructEntity(annotationClass);
    }
}
