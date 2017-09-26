package com.infomaximum.database.core.iterator;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DataSourceDatabaseException;

/**
 * Created by kris on 08.09.17.
 */
public interface IteratorEntity<E extends DomainObject> extends AutoCloseable {

    boolean hasNext();

    E next() throws DataSourceDatabaseException;

    void close() throws DataSourceDatabaseException;

}
