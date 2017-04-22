package com.infomaximum.rocksdb.core.objectsource;

import javassist.util.proxy.MethodHandler;

import java.lang.reflect.Method;

/**
 * Created by kris on 29.12.16.
 */
public class DomainObjectMethodHandler implements MethodHandler {

    public DomainObjectMethodHandler() {

    }

    @Override
    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
        return null;
    }
}
