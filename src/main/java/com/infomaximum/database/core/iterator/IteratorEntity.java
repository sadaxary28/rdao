package com.infomaximum.database.core.iterator;

import com.infomaximum.database.domainobject.DomainObject;

import java.util.Iterator;

/**
 * Created by kris on 08.09.17.
 */
public interface IteratorEntity<E extends DomainObject> extends Iterator<E>, Iterable<E>{

    boolean hasNext();

    E next();

    Iterator<E> iterator();

}
