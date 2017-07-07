package com.infomaximum.rocksdb.utils;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.util.proxy.ProxyObject;

/**
 * Created by kris on 07.07.17.
 */
public class ProxyDomainObjectUtils {

    public static Class<? extends DomainObject> getProxySuperClass(Class<? extends DomainObject> clazz) {
        if (ProxyObject.class.isAssignableFrom(clazz)) {
            return getProxySuperClass((Class<? extends DomainObject>) clazz.getSuperclass());
        } else {
            return clazz;
        }
    }

}
