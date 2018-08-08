package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.exception.DatabaseException;

import java.lang.reflect.Constructor;
import java.util.NoSuchElementException;
import java.util.Set;

public class AllIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Constructor<E> constructor;
    private final Set<Integer> loadingFields;
    private final DBIterator dataIterator;

    private final DataEnumerable.NextState state;

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<Integer> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.constructor = DomainObject.getConstructor(clazz);
        this.loadingFields = loadingFields;
        StructEntity entity = Schema.getEntity(clazz);
        this.dataIterator = dataEnumerable.createIterator(entity.getColumnFamily());

        KeyPattern dataKeyPattern = loadingFields != null ? FieldKey.buildKeyPattern(entity.getFieldNames(loadingFields)) : null;
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

        return dataEnumerable.nextObject(constructor, loadingFields, dataIterator, state);
    }

    @Override
    public void close() throws DatabaseException {
        dataIterator.close();
    }
}
