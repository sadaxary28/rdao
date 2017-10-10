package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.iterator.IndexIterator;
import com.infomaximum.database.core.iterator.AllIterator;
import com.infomaximum.database.core.iterator.PrefixIndexIterator;
import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.filter.Filter;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.Collections;
import java.util.Set;

public abstract class DataEnumerable {

    final DataSource dataSource;

    DataEnumerable(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public abstract <T extends Object, U extends DomainObject> T getValue(final EntityField field, U object) throws DataSourceDatabaseException;
    public abstract long createIterator(String columnFamily, final KeyPattern pattern) throws DataSourceDatabaseException;

    public void seekIterator(long iteratorId, final KeyPattern pattern) throws DataSourceDatabaseException {
        dataSource.seekIterator(iteratorId, pattern);
    }

    public KeyValue next(long iteratorId) throws DataSourceDatabaseException {
        return dataSource.next(iteratorId);
    }

    public void closeIterator(long iteratorId) {
        dataSource.closeIterator(iteratorId);
    }

    public <T extends DomainObject> T get(final Class<T> clazz, long id, final Set<String> loadingFields) throws DataSourceDatabaseException {
        String columnFamily = StructEntity.getInstance(clazz).getColumnFamily();
        KeyPattern pattern = FieldKey.buildKeyPattern(id, loadingFields != null ? loadingFields : Collections.emptySet());

        long iteratorId = createIterator(columnFamily, pattern);

        T obj;
        try {
            obj = DomainObjectUtils.nextObject(clazz, this, iteratorId, null);
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        return obj;
    }

    public <T extends DomainObject> T get(final Class<T> clazz, long id) throws DataSourceDatabaseException {
        return get(clazz, id, Collections.emptySet());
    }

    public <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, Filter filter, final Set<String> loadingFields) throws DatabaseException {
        if (filter instanceof EmptyFilter) {
            return new AllIterator(this, clazz, loadingFields);
        } else if (filter instanceof IndexFilter) {
            return new IndexIterator(this, clazz, loadingFields, (IndexFilter)filter);
        } else if (filter instanceof PrefixIndexFilter) {
            return new PrefixIndexIterator( this, clazz, loadingFields, (PrefixIndexFilter)filter);
        }
        throw new IllegalArgumentException("Unknown filter type " + filter.getClass());
    }

    public <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, Filter filter) throws DatabaseException {
        return find(clazz, filter, Collections.emptySet());
    }
}
