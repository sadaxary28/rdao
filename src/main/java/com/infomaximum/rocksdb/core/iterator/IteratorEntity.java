package com.infomaximum.rocksdb.core.iterator;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import org.rocksdb.RocksDBException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntity<E extends DomainObject> implements Iterator<E>, Iterable<E> {

    private final DataSource dataSource;
    private final Class<E> clazz;
    private final String columnFamily;

    private E nextElement;

    public IteratorEntity(DataSource dataSource, Class<E> clazz) throws ReflectiveOperationException, RocksDBException {
        this.dataSource = dataSource;
        this.clazz = clazz;

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);
        this.columnFamily = entityAnnotation.name();

        nextElement = loadNextElement(true);
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(boolean isFirst) throws RocksDBException, ReflectiveOperationException {
        Long prevId = (isFirst)?null:nextElement.getId();

        EntitySource entitySource = dataSource.nextEntitySource(columnFamily, prevId, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
        if (entitySource==null) {
            nextElement = null;
        } else {
            nextElement = DomainObjectUtils.createDomainObject(dataSource, clazz, entitySource);
        }
        return nextElement;
    }

    @Override
    public boolean hasNext() {
        return (nextElement!=null);
    }

    @Override
    public E next() {
        if (nextElement==null) throw new NoSuchElementException();

        E element = nextElement;
        try {
            nextElement = loadNextElement(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return element;
    }

    @Override
    public Iterator<E> iterator() {
        return this;
    }

}
