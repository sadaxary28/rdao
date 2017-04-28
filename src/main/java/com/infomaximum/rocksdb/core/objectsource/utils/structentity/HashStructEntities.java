package com.infomaximum.rocksdb.core.objectsource.utils.structentity;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

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


    private static Field transactionField;
    static {
        try {
            transactionField = DomainObject.class.getDeclaredField("transaction");
            transactionField.setAccessible(true);
        } catch (Exception e) {
            log.error("Exception find field: transaction");
        }
    }
    public static Field getTransactionField(){
        return transactionField;
    }


    private static Field lazyLoadsField;
    static {
        try {
            lazyLoadsField = DomainObject.class.getDeclaredField("lazyLoads");
            lazyLoadsField.setAccessible(true);
        } catch (Exception e) {
            log.error("Exception find field: lazyLoads");
        }
    }
    public static Field getLazyLoadsField(){
        return lazyLoadsField;
    }


    private static Map<Class, StructEntity> structEntities = new HashMap<Class, StructEntity>();
    public static StructEntity getStructEntity(Class clazz) {
        StructEntity domainObjectFields = structEntities.get(clazz);
        if (domainObjectFields==null){
            synchronized (structEntities) {
                domainObjectFields = structEntities.get(clazz);
                if (domainObjectFields==null){
                    domainObjectFields = new StructEntity(clazz);
                    structEntities.put(clazz, domainObjectFields);
                }
            }
        }
        return domainObjectFields;
    }

}
