package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.EntitySource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;

import java.util.NoSuchElementException;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntityImpl<E extends DomainObject> implements IteratorEntity<E> {

    private final DataSource dataSource;
    private final Class<E> clazz;
    private final String columnFamily;
    private final StructEntity structEntity;
    private final long iteratorId;

    private E nextElement;
    private EntitySource[] state = new EntitySource[1];

    public IteratorEntityImpl(DataSource dataSource, Class<E> clazz) throws DatabaseException {
        this.dataSource = dataSource;
        this.clazz = clazz;
        this.structEntity = HashStructEntities.getStructEntity(clazz);
        this.columnFamily = structEntity.annotationEntity.name();
        this.iteratorId = dataSource.createIterator(columnFamily);

        nextElement = loadNextElement();
    }

    private E loadNextElement() throws DataSourceDatabaseException {
        EntitySource entitySource = DomainObjectUtils.nextEntitySource(dataSource, iteratorId, structEntity.getEagerFormatFieldNames(), state);
        if (entitySource == null) {
            nextElement = null;
        } else {
            nextElement = DomainObjectUtils.buildDomainObject(dataSource, clazz, entitySource);
        }
        return nextElement;
    }

    @Override
    public boolean hasNext() {
        return (nextElement != null);
    }

    @Override
    public E next() throws DataSourceDatabaseException {
        if (nextElement == null) {
            throw new NoSuchElementException();
        }

        E element = nextElement;
        nextElement = loadNextElement();
        if (nextElement == null) {
            close();
        }

        return element;
    }

    @Override
    public void close() {
        dataSource.closeIterator(iteratorId);
    }
}
