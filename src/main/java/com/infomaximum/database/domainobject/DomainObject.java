package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.runtime.FieldValueNotFoundException;
import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.StructEntity;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.*;

public abstract class DomainObject implements Serializable {

    private final long id;
    private Map<String, Optional<Object>> loadedFieldValues;
    private Map<String, Object> newFieldValues = null;

    private transient StructEntity lazyStructEntity;

    public DomainObject(long id) {
        if (id < 1) {
            throw new IllegalArgumentException("id = " + Long.toString(id));
        }
        this.id = id;
        this.loadedFieldValues = new HashMap<>();
    }

    public long getId() {
        return id;
    }

    public <T> T get(String fieldName) {
        if (newFieldValues != null && newFieldValues.containsKey(fieldName)) {
            return (T) newFieldValues.get(fieldName);
        }

        Optional<Object> value = loadedFieldValues.get(fieldName);
        if (value == null) {
            throw new FieldValueNotFoundException(fieldName);
        }

        return (T) value.orElse(null);
    }

    protected void set(String fieldName, Object value) {
        if (newFieldValues == null) {
            newFieldValues = new HashMap<>();
        }

        EntityField field = getStructEntity().getField(fieldName);

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
        return get(fieldName);
    }

    protected Integer getInteger(String fieldName) {
        return get(fieldName);
    }

    protected Long getLong(String fieldName) {
        return get(fieldName);
    }

    protected Date getDate(String fieldName) {
        return get(fieldName);
    }

    protected Boolean getBoolean(String fieldName) {
        return get(fieldName);
    }

    protected byte[] getBytes(String fieldName) {
        return get(fieldName);
    }

    StructEntity getStructEntity() {
        if (lazyStructEntity == null) {
            lazyStructEntity = Schema.getEntity(this.getClass());
        }
        return lazyStructEntity;
    }

    protected Map<EntityField, Object> getLoadedValues(){
        Map<EntityField, Object> values = new HashMap<>(loadedFieldValues.size());
        for (Map.Entry<String, Optional<Object>> entry: loadedFieldValues.entrySet()){
            values.put(getStructEntity().getField(entry.getKey()), entry.getValue().orElse(null));
        }
        return values;
    }

    protected Map<EntityField, Object> getNewValues(){
        if (newFieldValues == null || newFieldValues.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<EntityField, Object> values = new HashMap<>(newFieldValues.size());
        for (Map.Entry<String, Object> entry: newFieldValues.entrySet()){
            values.put(getStructEntity().getField(entry.getKey()), entry.getValue());
        }
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || !(o instanceof DomainObject)) return false;

        DomainObject that = (DomainObject) o;

        return getStructEntity() == that.getStructEntity() &&
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
