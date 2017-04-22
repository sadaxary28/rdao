package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.util.proxy.MethodHandler;

import java.lang.reflect.Method;

/**
 * Created by kris on 29.12.16.
 */
public class MethodHandlerImpl implements MethodHandler {

    private final Class<? extends DomainObject> clazz;

    public MethodHandlerImpl(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
        if ("save".equals(thisMethod.getName())) {
            saveDomainObject((DomainObject) self);
            return null;
        } else {
            throw new RuntimeException("Not handler method: " + thisMethod.getName());
        }
    }

    private void saveDomainObject(DomainObject self){
        if (self.isReadOnly()) throw new RuntimeException("DomainObject: " + self + " load in readonly mode");
    }
}
