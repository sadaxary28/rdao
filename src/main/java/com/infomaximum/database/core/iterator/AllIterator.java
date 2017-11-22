package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.NoSuchElementException;
import java.util.Set;

public class AllIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Class<E> clazz;
    private final Set<String> loadingFields;
    private final long dataIteratorId;

    private E nextElement;
    private DataEnumerable.NextState state = new DataEnumerable.NextState();

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.clazz = clazz;
        this.loadingFields = loadingFields;
        String columnFamily = Schema.getEntity(clazz).getColumnFamily();
        this.dataIteratorId = dataEnumerable.createIterator(columnFamily);

        nextElement = dataEnumerable.seekObject(clazz, loadingFields, dataIteratorId, FieldKey.buildKeyPattern(loadingFields), state);
        if (nextElement == null) {
            close();
        }
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
    public void close() {
        dataEnumerable.closeIterator(dataIteratorId);
    }

    private void nextImpl() throws DataSourceDatabaseException {
        nextElement = dataEnumerable.nextObject(clazz, loadingFields, dataIteratorId, state);
        if (nextElement == null) {
            close();
        }
    }
}
