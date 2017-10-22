package com.infomaximum.database.core.schema;

import com.infomaximum.database.domainobject.DomainObject;

import java.util.*;

public class Schema {

    public static class Builder {

        private final Set<Class<? extends DomainObject>> domainClasses = new HashSet<>();

        public Builder withDomain(Class<? extends DomainObject> clazz) {
            if (!domainClasses.add(clazz)) {
                throw new RuntimeException("Class " + clazz + " already exists.");
            }
            return this;
        }

        public Schema build() {
            return new Schema(domainClasses);
        }
    }

    private final static Map<Class<? extends DomainObject>, StructEntity> entities = new HashMap<>();

    private final Set<StructEntity> domains;

    private Schema(Set<Class<? extends DomainObject>> domainClasses) {
        Set<StructEntity> modifiableDomains = new HashSet<>(domainClasses.size());
        for (Class domain : domainClasses) {
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
        return entities.get(StructEntity.getAnnotationClass(clazz));
    }

    static StructEntity ensureEntity(Class<? extends DomainObject> domain) {
        Class annotationClass = StructEntity.getAnnotationClass(domain);
        StructEntity entity = entities.get(annotationClass);
        if (entity == null) {
            entity = new StructEntity(annotationClass);
            entities.put(annotationClass, entity);
        }
        return entity;
    }
}
