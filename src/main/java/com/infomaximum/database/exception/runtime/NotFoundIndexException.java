package com.infomaximum.database.exception.runtime;

import com.infomaximum.database.domainobject.DomainObject;

import java.util.Collection;

/**
 * Created by kris on 06.09.17.
 */
public class NotFoundIndexException extends RuntimeException {

    public NotFoundIndexException(Class<? extends DomainObject> clazz, Collection<String> fileds) {
        super("Not found index for fields [" + String.join(", ", fileds) + "] in " + clazz);
    }

}

