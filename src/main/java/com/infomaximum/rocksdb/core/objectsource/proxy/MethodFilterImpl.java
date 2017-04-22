package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.util.proxy.MethodFilter;

import java.lang.reflect.Method;

/**
 * Created by kris on 22.04.17.
 */
public class MethodFilterImpl implements MethodFilter {

    private final Class<? extends DomainObject> clazz;

    public MethodFilterImpl(Class<? extends DomainObject> clazz) {
        this.clazz = clazz;
    }

    @Override
    public boolean isHandled(Method m) {
        if ("save".equals(m.getName())) {
            //Вот save мы ловим ставим свой обработчик
            return true;
        } else {
            return false;
        }
    }
}
