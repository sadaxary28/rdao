package com.infomaximum.database.core.structentity;

import com.infomaximum.database.domainobject.DomainObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by user on 23.04.2017.
 */
public class HashStructEntities {

    private final static Logger log = LoggerFactory.getLogger(HashStructEntities.class);

    public final static Field dataSourceField = getDataSourceField();
    private final static ConcurrentMap<Class<? extends DomainObject>, StructEntity> structEntities = new ConcurrentHashMap<>();

    public static StructEntity getStructEntity(Class<? extends DomainObject> clazz) {
        Class<? extends DomainObject> entityClass = StructEntity.getEntityClass(clazz);

        StructEntity domainObjectFields = structEntities.get(entityClass);
        if (domainObjectFields != null) {
            return domainObjectFields;
        }

        StructEntity newValue = new StructEntity(entityClass);
        domainObjectFields = structEntities.putIfAbsent(entityClass, newValue);
        return domainObjectFields != null ? domainObjectFields : newValue;
    }

    private static Field getDataSourceField() {
        Field field = null;
        try {
            field = DomainObject.class.getDeclaredField("dataSource");
            field.setAccessible(true);
        } catch (Exception e) {
            log.error("Exception HashStructEntities.getDataSourceField", e);
        }

        return field;
    }
}
