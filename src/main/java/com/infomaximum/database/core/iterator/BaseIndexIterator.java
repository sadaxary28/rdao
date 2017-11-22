package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

import java.util.*;


public abstract class BaseIndexIterator<E extends DomainObject> implements IteratorEntity<E> {

    final DataEnumerable dataEnumerable;
    final Class<E> clazz;
    final Set<String> loadingFields;

    long indexIteratorId = -1;
    KeyPattern dataKeyPattern = null;
    long dataIteratorId = -1;
    E nextElement;

    BaseIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields) throws DataSourceDatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.clazz = clazz;
        this.loadingFields = loadingFields;
    }

    @Override
    public boolean hasNext() {
        return nextElement != null;
    }

    @Override
    public E next() throws DataSourceDatabaseException {
        if (nextElement == null) {
            throw new NoSuchElementException();
        }

        E element = nextElement;
        nextImpl();
        return element;
    }

    @Override
    public void close() throws DataSourceDatabaseException {
        dataEnumerable.closeIterator(indexIteratorId);
        if (dataIteratorId != -1) {
            dataEnumerable.closeIterator(dataIteratorId);
        }
    }

    abstract void nextImpl() throws DataSourceDatabaseException;

    static KeyPattern buildDataKeyPattern(List<EntityField> fields1, Set<String> fields2) {
        if (fields2 == null) {
            fields2 = Collections.emptySet();
        }

        if (fields1 == null || fields1.isEmpty()) {
            return fields2.isEmpty() ? null : FieldKey.buildKeyPattern(fields2);
        }

        Set<String> fields = new HashSet<>(fields1.size() + fields2.size());
        fields1.forEach(field -> fields.add(field.getName()));
        fields.addAll(fields2);
        return FieldKey.buildKeyPattern(fields);
    }

    E findObject(long id) throws DataSourceDatabaseException {
        if (dataKeyPattern == null) {
            return dataEnumerable.buildDomainObject(clazz, id, loadingFields);
        }

        dataKeyPattern.setPrefix(FieldKey.buildKeyPrefix(id));

        E obj = dataEnumerable.seekObject(clazz, loadingFields, dataIteratorId, dataKeyPattern, null);
        return checkFilter(obj) ? obj : null;
    }

    abstract boolean checkFilter(E obj) throws DataSourceDatabaseException;
}

