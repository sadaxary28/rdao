package com.infomaximum.rocksdb.exception;

import com.infomaximum.rocksdb.core.struct.DomainObject;

import java.util.Collection;

/**
 * Created by kris on 27.06.17.
 */
public class NotFoundIndexException extends RdaoRuntimeException {

    private final Class<? extends DomainObject> clazz;

    public NotFoundIndexException(Class<? extends DomainObject> clazz, Collection<String> nameFileds) {
        super("Not found index to fields [" + String.join(", ", nameFileds) + "], to " + clazz.getName());
        this.clazz = clazz;
    }
}
