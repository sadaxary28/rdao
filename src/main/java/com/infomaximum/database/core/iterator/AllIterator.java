package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

public class AllIterator<E extends DomainObject> implements IteratorEntity<E> {

    private final DataEnumerable dataEnumerable;
    private final Class<E> clazz;
    private final long dataIteratorId;

    private E nextElement;
    private DomainObjectUtils.NextState state = new DomainObjectUtils.NextState();

    public AllIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields) throws DatabaseException {
        this.dataEnumerable = dataEnumerable;
        this.clazz = clazz;
        String columnFamily = StructEntity.getInstance(clazz).getName();
        this.dataIteratorId = dataEnumerable.createIterator(columnFamily, FieldKey.buildKeyPattern(loadingFields));

        nextImpl();
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
        nextElement = DomainObjectUtils.nextObject(clazz, dataEnumerable, dataIteratorId, state);
        if (nextElement == null) {
            close();
        }
    }
}
