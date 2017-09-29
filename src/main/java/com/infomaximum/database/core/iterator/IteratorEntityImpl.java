package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.structentity.StructEntity;
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

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntityImpl<E extends DomainObject> implements IteratorEntity<E> {

    private final DataSource dataSource;
    private final DataEnumerable dataEnumerable;
    private final Class<E> clazz;
    private final long iteratorId;

    private E nextElement;
    private DomainObjectUtils.NextState state = new DomainObjectUtils.NextState();

    public IteratorEntityImpl(DataSource dataSource, DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, long transactionId) throws DatabaseException {
        this.dataSource = dataSource;
        this.dataEnumerable = dataEnumerable;
        this.clazz = clazz;
        String columnFamily = StructEntity.getInstance(clazz).annotationEntity.name();
        KeyPattern keyPattern = FieldKey.buildKeyPattern(loadingFields != null ? loadingFields : Collections.emptySet());
        if (transactionId == -1) {
            this.iteratorId = dataSource.createIterator(columnFamily, keyPattern);
        } else {
            this.iteratorId = dataSource.createIterator(columnFamily, keyPattern, transactionId);
        }

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
        nextElement = DomainObjectUtils.nextObject(clazz, dataSource, iteratorId, dataEnumerable, state);
        if (nextElement == null) {
            close();
        }
    }
}
