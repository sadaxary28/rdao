package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.entitysource.EntitySource;
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

    private final long iteratorId;

    private E nextElement;

    public IteratorEntityImpl(DataSource dataSource, Class<E> clazz) throws DatabaseException {
        this.dataSource = dataSource;
        this.clazz = clazz;

        StructEntity structEntity = HashStructEntities.getStructEntity(clazz);
        Entity entityAnnotation = structEntity.annotationEntity;
        this.columnFamily = entityAnnotation.name();

        this.iteratorId = dataSource.createIterator(columnFamily);

        nextElement = loadNextElement();
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement() throws DataSourceDatabaseException {
        EntitySource entitySource = dataSource.nextEntitySource(iteratorId, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
        if (entitySource == null) {
            nextElement = null;
        } else {
            nextElement = DomainObjectUtils.buildDomainObject(dataSource, clazz, entitySource);
        }
        return nextElement;
    }

    @Override
    public boolean hasNext() {
        return (nextElement!=null);
    }

    @Override
    public E next() throws DataSourceDatabaseException {
        if (nextElement==null) throw new NoSuchElementException();

        E element = nextElement;
        nextElement = loadNextElement();

        //Если элементы закончились - сразу закрываем
        if (nextElement==null) close();

        return element;
    }

    @Override
    public void close() {
        dataSource.closeIterator(iteratorId);
    }
}
