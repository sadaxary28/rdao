package com.infomaximum.rocksdb.core.objectsource.utils;

import com.google.common.base.CaseFormat;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by user on 23.04.2017.
 */
public class HashFields {

    private final static Logger log = LoggerFactory.getLogger(HashFields.class);

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




    private static Map<Class, Map<String, Field>> entityFields = new HashMap<Class, Map<String, Field>>();

    public static Field getEntityField(Class clazz, String fieldName) throws NoSuchFieldException {
        Map<String, Field> fields = getEntityFields(clazz);
        return fields.get(fieldName);
    }

    public static Collection<String> getEntityFieldNames(Class clazz) throws NoSuchFieldException {
        Map<String, Field> fields = getEntityFields(clazz);
        return fields.keySet();
    }

    private static Map<String, Field> getEntityFields(Class clazz) {
        Map<String, Field> fields = entityFields.get(clazz);
        if (fields==null){
            synchronized (entityFields) {
                fields = entityFields.get(clazz);
                if (fields==null){
                    fields = new HashMap<String, Field>();
                    entityFields.put(clazz, fields);

                    for (Field field: clazz.getDeclaredFields()) {
                        EntityField entityField = field.getAnnotation(EntityField.class);
                        if (entityField==null) continue;

                        String fieldName = entityField.name();
                        if (fieldName.isEmpty()) fieldName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());

                        field.setAccessible(true);
                        fields.put(fieldName, field);
                    }
                }
            }
        }
        return fields;
    }

}
