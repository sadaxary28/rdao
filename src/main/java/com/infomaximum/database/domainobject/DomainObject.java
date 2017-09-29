package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.IllegalTypeDatabaseException;
import com.infomaximum.database.utils.BaseEnum;
import com.infomaximum.database.utils.EqualsUtils;

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

    private DataEnumerable dataSource = null;
    private ConcurrentMap<String, Optional<Object>> loadedFieldValues = null;
    private Map<String, Object> newFieldValues = null;

    public DomainObject(long id) {
        if (id < 1) {
            throw new IllegalArgumentException();
        }
        this.id = id;
        this.structEntity = StructEntity.getInstance(this.getClass());
        this.loadedFieldValues = new ConcurrentHashMap<>();
    }

    public long getId() {
        return id;
    }

    public <T> T get(Class<T> type, String fieldName) throws DataSourceDatabaseException {
        if (newFieldValues != null && newFieldValues.containsKey(fieldName)) {
            return (T) newFieldValues.get(fieldName);
        } else if (loadedFieldValues.containsKey(fieldName)) {
            return (T) loadedFieldValues.get(fieldName).orElse(null);
        } else {
            T value = dataSource.getField(type, fieldName, this);
            Optional<Object> prevValue = loadedFieldValues.putIfAbsent(fieldName, Optional.ofNullable(value));
            return prevValue != null ? (T) prevValue.orElse(null) : value;
        }
    }

    protected void set(String field, Object value) {
        if (newFieldValues == null) {
            newFieldValues = new HashMap<>();
        }

        Field aField = structEntity.getFieldByName(field);

        //Проверяем на совпадение типов
        if (value != null && !EqualsUtils.equalsType(value.getClass(), aField.type())) {
            throw new IllegalTypeDatabaseException("Not equals type field in type value");
        }

        newFieldValues.put(field, value);
    }

    /**
     * Unsafe method. Do not use in external packages!
     * @param name
     * @param value
     */
    protected void _setLoadedField(String name, Object value) {
        loadedFieldValues.put(name, Optional.ofNullable(value));
    }

    /**
     * Unsafe method. Do not use in external packages!
     */
    protected void _flushNewValues() {
        for (Map.Entry<String, Object> entry : newFieldValues.entrySet()){
            _setLoadedField(entry.getKey(), entry.getValue());
        }
        newFieldValues.clear();
    }

    protected String getString(String fieldName) throws DataSourceDatabaseException {
        return get(String.class, fieldName);
    }

    protected Integer getInteger(String fieldName) throws DataSourceDatabaseException {
        return get(Integer.class, fieldName);
    }

    protected Long getLong(String fieldName) throws DataSourceDatabaseException {
        return get(Long.class, fieldName);
    }

    protected Date getDate(String fieldName) throws DataSourceDatabaseException {
        return get(Date.class, fieldName);
    }

    protected Boolean getBoolean(String fieldName) throws DataSourceDatabaseException {
        return get(Boolean.class, fieldName);
    }

    protected byte[] getBytes(String fieldName) throws DataSourceDatabaseException {
        return get(byte[].class, fieldName);
    }

    protected <T extends Enum & BaseEnum> T getEnum(Class<T> enumClass, String fieldName) throws DataSourceDatabaseException {
        return get(enumClass, fieldName);
    }

    protected StructEntity getStructEntity() {
        return structEntity;
    }

    protected Map<Field, Object> getLoadedValues(){
        Map<Field, Object> values = new HashMap<>(loadedFieldValues.size());
        for (Map.Entry<String, Optional<Object>> entry: loadedFieldValues.entrySet()){
            values.put(structEntity.getFieldByName(entry.getKey()), entry.getValue().orElse(null));
        }
        return values;
    }

    protected Map<Field, Object> getNewValues(){
        Map<Field, Object> values = new HashMap<>(newFieldValues.size());
        for (Map.Entry<String, Object> entry: newFieldValues.entrySet()){
            values.put(structEntity.getFieldByName(entry.getKey()), entry.getValue());
        }
        return values;
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
}
