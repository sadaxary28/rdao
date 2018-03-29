package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.runtime.FieldValueNotFoundException;
import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.StructEntity;

import com.infomaximum.database.exception.runtime.IllegalTypeException;

import java.lang.reflect.Constructor;
import java.util.*;

public abstract class DomainObject {

    private final long id;
    private final StructEntity structEntity;
    private Map<String, Optional<Object>> loadedFieldValues;
    private Map<String, Object> newFieldValues = null;

    public DomainObject(long id) {
        if (id < 1) {
            throw new IllegalArgumentException("id = " + Long.toString(id));
        }
        this.id = id;
        this.structEntity = Schema.getEntity(this.getClass());
        this.loadedFieldValues = new HashMap<>();
    }

    public long getId() {
        return id;
    }

    public <T> T get(Class<T> type, String fieldName) {
        if (newFieldValues != null && newFieldValues.containsKey(fieldName)) {
            return (T) newFieldValues.get(fieldName);
        }

        Optional<Object> value = loadedFieldValues.get(fieldName);
        if (value == null) {
            throw new FieldValueNotFoundException(fieldName);
        }

        return (T) value.orElse(null);
    }

    public Object get(EntityField field) {
        return get(Object.class, field.getName());
    }

    protected void set(String fieldName, Object value) {
        if (newFieldValues == null) {
            newFieldValues = new HashMap<>();
        }

        EntityField field = structEntity.getField(fieldName);

        if (value != null) {
            field.throwIfNotMatch(value.getClass());
        }

        newFieldValues.put(fieldName, value);
    }

    /**
     * Unsafe method. Do not use in external packages!
     */
    void _setLoadedField(String name, Object value) {
        loadedFieldValues.put(name, Optional.ofNullable(value));
    }

    /**
     * Unsafe method. Do not use in external packages!
     */
    void _flushNewValues() {
        if (newFieldValues == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : newFieldValues.entrySet()){
            _setLoadedField(entry.getKey(), entry.getValue());
        }
        newFieldValues.clear();
    }

    protected String getString(String fieldName) {
        return get(String.class, fieldName);
    }

    protected Integer getInteger(String fieldName) {
        return get(Integer.class, fieldName);
    }

    protected Long getLong(String fieldName) {
        return get(Long.class, fieldName);
    }

    protected Date getDate(String fieldName) {
        return get(Date.class, fieldName);
    }

    protected Boolean getBoolean(String fieldName) {
        return get(Boolean.class, fieldName);
    }

    protected byte[] getBytes(String fieldName) {
        return get(byte[].class, fieldName);
    }

    public StructEntity getStructEntity() {
        return structEntity;
    }

    protected Map<EntityField, Object> getLoadedValues(){
        Map<EntityField, Object> values = new HashMap<>(loadedFieldValues.size());
        for (Map.Entry<String, Optional<Object>> entry: loadedFieldValues.entrySet()){
            values.put(structEntity.getField(entry.getKey()), entry.getValue().orElse(null));
        }
        return values;
    }

    protected Map<EntityField, Object> getNewValues(){
        if (newFieldValues == null || newFieldValues.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<EntityField, Object> values = new HashMap<>(newFieldValues.size());
        for (Map.Entry<String, Object> entry: newFieldValues.entrySet()){
            values.put(structEntity.getField(entry.getKey()), entry.getValue());
        }
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof DomainObject)) return false;

        DomainObject that = (DomainObject) o;

        return structEntity == that.structEntity &&
               id == that.id;
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

    public static <T extends DomainObject> Constructor<T> getConstructor(Class<T> clazz) {
        try {
            return clazz.getConstructor(long.class);
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeException(e);
        }
    }
}
