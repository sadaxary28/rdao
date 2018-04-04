package com.infomaximum.database.domainobject;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.domainobject.iterator.*;
import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.domainobject.filter.*;
import com.infomaximum.database.utils.key.FieldKey;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.UnexpectedEndObjectException;
import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.utils.TypeConvert;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Set;

public abstract class DataEnumerable {

    public static class NextState {

        private long nextId = -1;

        private NextState(long recordId) {
            this.nextId = recordId;
        }

        public boolean isEmpty() {
            return nextId == -1;
        }
    }

    protected final DBProvider dbProvider;

    DataEnumerable(DBProvider dbProvider) {
        this.dbProvider = dbProvider;
    }

    public DBProvider getDbProvider() {
        return dbProvider;
    }

    public abstract <T, U extends DomainObject> T getValue(final EntityField field, U object) throws DatabaseException;
    public abstract DBIterator createIterator(String columnFamily) throws DatabaseException;

    public <T extends DomainObject> T get(final Class<T> clazz, long id, final Set<String> loadingFields) throws DatabaseException {
        final String columnFamily = Schema.getEntity(clazz).getColumnFamily();

        try (DBIterator iterator = createIterator(columnFamily)) {
            return seekObject(DomainObject.getConstructor(clazz), loadingFields, iterator, FieldKey.buildKeyPattern(id, loadingFields));
        }
    }

    public <T extends DomainObject> T get(final Class<T> clazz, long id) throws DatabaseException {
        return get(clazz, id, null);
    }

    public <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, Filter filter, final Set<String> loadingFields) throws DatabaseException {
        if (filter instanceof EmptyFilter) {
            return new AllIterator<>(this, clazz, loadingFields);
        } else if (filter instanceof IndexFilter) {
            return new IndexIterator<>(this, clazz, loadingFields, (IndexFilter)filter);
        } else if (filter instanceof PrefixIndexFilter) {
            return new PrefixIndexIterator<>( this, clazz, loadingFields, (PrefixIndexFilter)filter);
        } else if (filter instanceof IntervalIndexFilter) {
            return new IntervalIndexIterator<>(this, clazz, loadingFields, (IntervalIndexFilter) filter);
        }
        throw new IllegalArgumentException("Unknown filter type " + filter.getClass());
    }

    public <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, Filter filter) throws DatabaseException {
        return find(clazz, filter, null);
    }

    public <T extends DomainObject> T buildDomainObject(final Constructor<T> constructor, long id, Collection<String> preInitializedFields) {
        T obj = buildDomainObject(constructor, id);
        if (preInitializedFields == null) {
            for (EntityField field : obj.getStructEntity().getFields()) {
                obj._setLoadedField(field.getName(), null);
            }
        } else {
            for (String field : preInitializedFields) {
                obj._setLoadedField(field, null);
            }
        }
        return obj;
    }

    private <T extends DomainObject> T buildDomainObject(final Constructor<T> constructor, long id) {
        try {
            return constructor.newInstance(id);
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeException(e);
        }
    }

    public <T extends DomainObject> T nextObject(final Constructor<T> constructor, Collection<String> preInitializedFields,
                                                 DBIterator iterator, NextState state) throws DatabaseException {
        if (state.isEmpty()) {
            return null;
        }

        T obj = buildDomainObject(constructor, state.nextId, preInitializedFields);
        state.nextId = readObject(obj, iterator);
        return obj;
    }

    public <T extends DomainObject> T seekObject(final Constructor<T> constructor, Collection<String> preInitializedFields,
                                                 DBIterator iterator, KeyPattern pattern) throws DatabaseException {
        KeyValue keyValue = iterator.seek(pattern);
        if (keyValue == null) {
            return null;
        }

        FieldKey key = FieldKey.unpack(keyValue.getKey());
        if (!key.isBeginningObject()) {
            return null;
        }

        T obj = buildDomainObject(constructor, key.getId(), preInitializedFields);
        readObject(obj, iterator);
        return obj;
    }

    public NextState seek(DBIterator iterator, KeyPattern pattern) throws DatabaseException {
        KeyValue keyValue = iterator.seek(pattern);
        if (keyValue == null) {
            return new NextState(-1);
        }

        FieldKey key = FieldKey.unpack(keyValue.getKey());
        if (!key.isBeginningObject()) {
            return new NextState(-1);
        }

        return new NextState(key.getId());
    }

    private <T extends DomainObject> long readObject(T obj, DBIterator iterator) throws DatabaseException {
        KeyValue keyValue;
        FieldKey key;
        while ((keyValue = iterator.next()) != null) {
            key = FieldKey.unpack(keyValue.getKey());
            if (key.getId() != obj.getId()) {
                if (!key.isBeginningObject()) {
                    throw new UnexpectedEndObjectException(obj.getId(), key);
                }
                return key.getId();
            }
            EntityField field = obj.getStructEntity().getField(key.getFieldName());
            obj._setLoadedField(key.getFieldName(), TypeConvert.unpack(field.getType(), keyValue.getValue(), field.getConverter()));
        }

        return -1;
    }
}
