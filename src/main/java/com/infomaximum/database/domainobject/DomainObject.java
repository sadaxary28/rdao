package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by kris on 06.09.17.
 */
public abstract class DomainObject {

    private final long id;

    private final StructEntity structEntity;

    private DataSource dataSource = null;
    private ConcurrentMap<String, Optional<Object>> fieldValues = null;
    private volatile ConcurrentMap<String, Optional<Object>> waitWriteFieldValues = null;

    public DomainObject(long id) {
        this.id = id;

        this.structEntity = HashStructEntities.getStructEntity(this.getClass());

        this.fieldValues = new ConcurrentHashMap<String, Optional<Object>>();
    }

    public long getId() {
        return id;
    }

    public <T> T get(Class<T> type, String fieldName) throws DataSourceDatabaseException {
        if (waitWriteFieldValues!=null && waitWriteFieldValues.containsKey(fieldName)) {
            return (T) waitWriteFieldValues.get(fieldName).orElse(null);
        } else if (fieldValues.containsKey(fieldName)) {
            return (T) fieldValues.get(fieldName).orElse(null);
        } else {
            synchronized (this) {
                if (!fieldValues.containsKey(fieldName)) {
                    T value = loadField(type, fieldName);
                    fieldValues.put(fieldName, Optional.ofNullable(value));
                    return value;
                } else {
                    return (T) fieldValues.get(fieldName).orElse(null);
                }
            }
        }
    }

    protected void set(String field, Object value) {
        if (waitWriteFieldValues == null) {
            synchronized (this) {
                if (waitWriteFieldValues == null) {
                    waitWriteFieldValues = new ConcurrentHashMap<String, Optional<Object>>();
                }
            }
        }
        waitWriteFieldValues.put(field, Optional.ofNullable(value));
    }

    private <T> T loadField(Class<T> type, String fieldName) throws DataSourceDatabaseException {
        byte[] bValue = dataSource.getField(structEntity.annotationEntity.name(), id, fieldName);
        return (T) TypeConvert.get(type, bValue);
    }

    protected String getString(String fieldName) throws DataSourceDatabaseException {
        return get(String.class, fieldName);
    }

    protected Long getLong(String fieldName) throws DataSourceDatabaseException {
        return get(Long.class, fieldName);
    }

    protected Date getDate(String fieldName) throws DataSourceDatabaseException {
        long timestamp = get(Long.class, fieldName);
        return new Date(timestamp);
    }

    protected Boolean getBoolean(String fieldName) throws DataSourceDatabaseException {
        return get(Boolean.class, fieldName);
    }


    protected <T extends Enum> T getEnum(Class<T> enumClass, String fieldName) throws DataSourceDatabaseException {
        return get(enumClass, fieldName);
    }

    protected StructEntity getStructEntity() {
        return structEntity;
    }

    protected Map<Field, Object> getLoadValues(){
        return packMapValue(structEntity, fieldValues);
    }

    protected Map<Field, Object> writeValues(){
        return packMapValue(structEntity, waitWriteFieldValues);
    }

    protected void flush() {
        for (Map.Entry<String, Optional<Object>> entry: waitWriteFieldValues.entrySet()){
            fieldValues.put(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (!DomainObject.class.isAssignableFrom(o.getClass())) return false;
        DomainObject that = (DomainObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(getClass().getSuperclass().getName()).append('(')
                .append("id: ").append(id)
                .append(')').toString();
    }

    private static Map<Field, Object> packMapValue(StructEntity structEntity, Map<String, Optional<Object>> source){
        Map<Field, Object> values = new HashMap<>();
        for (Map.Entry<String, Optional<Object>> entry: source.entrySet()){

            Field field=structEntity.getFieldByName(entry.getKey());
            if (field==null) throw new RuntimeException("Что то совсем плохо ошибка в логике - такого быть не должно");

            values.put(field, entry.getValue().orElse(null));
        }
        return values;
    }
}
