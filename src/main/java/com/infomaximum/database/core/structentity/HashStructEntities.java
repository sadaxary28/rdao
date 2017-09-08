package com.infomaximum.database.core.structentity;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by user on 23.04.2017.
 */
public class HashStructEntities {

    private final static Logger log = LoggerFactory.getLogger(HashStructEntities.class);


    private static Field dataSourceField;
    static {
        try {
            dataSourceField = DomainObject.class.getDeclaredField("dataSource");
            dataSourceField.setAccessible(true);
        } catch (Exception e) {
            log.error("Exception find field: transaction");
        }
    }
    public static Field getDataSourceField(){
        return dataSourceField;
    }


    private static Map<Class<? extends DomainObject>, StructEntity> structEntities = new ConcurrentHashMap<>();
    public static StructEntity getStructEntity(Class<? extends DomainObject> clazz) throws DatabaseException {
        Class<? extends DomainObject> entityClass = StructEntity.getEntityClass(clazz);

        StructEntity domainObjectFields = structEntities.get(entityClass);
        if (domainObjectFields==null){
            synchronized (structEntities) {
                domainObjectFields = structEntities.get(entityClass);
                if (domainObjectFields==null){
                    domainObjectFields = new StructEntity(entityClass);
                    structEntities.put(entityClass, domainObjectFields);
                }
            }
        }
        return domainObjectFields;
    }

}
