package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.provider.DBProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

    public void install() throws DatabaseException {
        for (StructEntity domain : domains) {
            dbSchema.createTable(domain);
        }
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
}
