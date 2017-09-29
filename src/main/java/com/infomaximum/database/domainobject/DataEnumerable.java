package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.Map;
import java.util.Set;

public interface DataEnumerable {

    <T extends Object, U extends DomainObject> T getField(final Class<T> type, String fieldName, U object) throws DataSourceDatabaseException;
    <T extends DomainObject> T get(final Class<T> clazz, final Set<String> loadingFields, long id) throws DataSourceDatabaseException;
    <T extends DomainObject> IteratorEntity<T> iterator(final Class<T> clazz, final Set<String> loadingFields) throws DatabaseException;
    <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, final Set<String> loadingFields, Map<String, Object> filters) throws DatabaseException;
}
