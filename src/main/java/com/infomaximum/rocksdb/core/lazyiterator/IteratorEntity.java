package com.infomaximum.rocksdb.core.lazyiterator;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import org.rocksdb.RocksDBException;

import java.lang.reflect.InvocationTargetException;
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
        this.columnFamily = entityAnnotation.columnFamily();

        nextElement = loadNextElement(true);
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(boolean isFirst) throws RocksDBException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Long prevId = (isFirst)?null:nextElement.getId();

        EntitySource entitySource = dataSource.next(columnFamily, prevId, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
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
