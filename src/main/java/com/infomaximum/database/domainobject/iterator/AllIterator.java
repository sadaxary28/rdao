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

    private E nextElement;
    private DataEnumerable.NextState state = new DataEnumerable.NextState();

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.constructor = DomainObject.getConstructor(clazz);
        this.loadingFields = loadingFields;
        String columnFamily = Schema.getEntity(clazz).getColumnFamily();
        this.dataIterator = dataEnumerable.createIterator(columnFamily);

        KeyPattern dataKeyPattern = loadingFields != null ? FieldKey.buildKeyPattern(loadingFields) : null;
        nextElement = dataEnumerable.seekObject(constructor, loadingFields, dataIterator, dataKeyPattern, state);
        if (nextElement == null) {
            close();
        }
    }

    @Override
    public boolean hasNext() {
        return nextElement != null;
    }

    @Override
    public E next() throws DatabaseException {
        if (nextElement == null) {
            throw new NoSuchElementException();
        }

        E element = nextElement;
        nextImpl();
        return element;
    }

    @Override
    public void close() throws DatabaseException {
        dataIterator.close();
    }

    private void nextImpl() throws DatabaseException {
        nextElement = dataEnumerable.nextObject(constructor, loadingFields, dataIterator, state);
        if (nextElement == null) {
            close();
        }
    }
}
