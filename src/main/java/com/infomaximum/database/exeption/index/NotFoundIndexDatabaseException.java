package com.infomaximum.database.exeption.index;

import com.infomaximum.database.domainobject.DomainObject;

import java.util.Collection;

/**
 * Created by kris on 06.09.17.
 */
public class NotFoundIndexDatabaseException extends IndexDatabaseException {

    private final Class<? extends DomainObject> clazz;

    public NotFoundIndexDatabaseException(Class<? extends DomainObject> clazz, Collection<String> fileds) {
        super("Not found index to fields [" + String.join(", ", fileds) + "], to " + clazz.getName());
        this.clazz = clazz;
    }

}

