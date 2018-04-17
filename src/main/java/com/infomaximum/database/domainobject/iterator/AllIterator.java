package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.exception.DatabaseException;

import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;
import java.util.Set;

public class AllIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Constructor<E> constructor;
    private final Set<String> loadingFields;
    private final DBIterator dataIterator;

    private final DataEnumerable.NextState state;

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.constructor = DomainObject.getConstructor(clazz);
        this.loadingFields = loadingFields;
        String columnFamily = Schema.getEntity(clazz).getColumnFamily();
        this.dataIterator = dataEnumerable.createIterator(columnFamily);

        KeyPattern dataKeyPattern = loadingFields != null ? FieldKey.buildKeyPattern(loadingFields) : null;
        this.state = dataEnumerable.seek(dataIterator, dataKeyPattern);
        if (this.state.isEmpty()) {
            close();
        }
    }

    @Override
    public boolean hasNext() {
        return !state.isEmpty();
    }

    @Override
    public E next() throws DatabaseException {
        if (state.isEmpty()) {
            throw new NoSuchElementException();
        }

        E result = dataEnumerable.nextObject(constructor, loadingFields, dataIterator, state);
        if (result == null) {
            close();
        }

        return result;
    }

    @Override
    public void close() throws DatabaseException {
        dataIterator.close();
    }
}
