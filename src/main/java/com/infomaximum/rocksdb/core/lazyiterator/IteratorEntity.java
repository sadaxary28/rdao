package com.infomaximum.rocksdb.core.lazyiterator;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.struct.DomainObject;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorEntity<E extends DomainObject> implements Iterator<E>, Iterable<E> {

    private final DataSource dataSource;
    private final String columnFamily;

    private E nextElement;

    public IteratorEntity(DataSource dataSource, Class<? extends DomainObject> clazz) {
        this.dataSource = dataSource;

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation==null) throw new RuntimeException("Not found 'Entity' annotation in class: " + clazz);
        this.columnFamily = entityAnnotation.columnFamily();

        nextElement = loadNextElement();
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(){
//        dataSource.
        return null;
    }

    @Override
    public boolean hasNext() {
        return (nextElement!=null);
    }

    @Override
    public E next() {
        if (nextElement==null) throw new NoSuchElementException();

        E element = nextElement;
        nextElement = loadNextElement();

        return element;
    }

    @Override
    public Iterator<E> iterator() {
        return this;
    }

}
