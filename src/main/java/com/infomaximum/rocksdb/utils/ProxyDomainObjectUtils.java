package com.infomaximum.rocksdb.utils;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.rocksdb.core.struct.DomainObjectOLD;
import javassist.util.proxy.ProxyObject;

/**
 * Created by kris on 07.07.17.
 */
public class ProxyDomainObjectUtils {

    public static Class<? extends DomainObjectOLD> getProxySuperClassOLD(Class<? extends DomainObjectOLD> clazz) {
        if (ProxyObject.class.isAssignableFrom(clazz)) {
            return getProxySuperClassOLD((Class<? extends DomainObjectOLD>) clazz.getSuperclass());
        } else {
            return clazz;
        }
    }

    public static Class<? extends DomainObject> getProxySuperClass(Class<? extends DomainObject> clazz) {
        if (ProxyObject.class.isAssignableFrom(clazz)) {
            return getProxySuperClass((Class<? extends DomainObject>) clazz.getSuperclass());
        } else {
            return clazz;
        }
    }

}
