package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntityImpl<E extends DomainObject> implements IteratorEntity<E> {

    private final DataSource dataSource;
    private final Class<E> clazz;
    private final long iteratorId;

    private E nextElement;
    private DomainObjectUtils.NextState state = new DomainObjectUtils.NextState();

    public IteratorEntityImpl(DataSource dataSource, Class<E> clazz, Set<String> loadingFields) throws DatabaseException {
        this.dataSource = dataSource;
        this.clazz = clazz;
        String columnFamily = HashStructEntities.getStructEntity(clazz).annotationEntity.name();
        this.iteratorId = dataSource.createIterator(columnFamily, FieldKey.buildKeyPattern(loadingFields));

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
        dataSource.closeIterator(iteratorId);
    }

    private void nextImpl() throws DataSourceDatabaseException {
        nextElement = DomainObjectUtils.nextObject(clazz, dataSource, iteratorId, state);
        if (nextElement == null) {
            close();
        }
    }
}
