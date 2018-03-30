package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DomainObject;

import com.infomaximum.database.exception.DatabaseException;

/**
 * Created by kris on 08.09.17.
 */
public interface IteratorEntity<E extends DomainObject> extends AutoCloseable {

    boolean hasNext();

    E next() throws DatabaseException;

    void close() throws DatabaseException;

}
