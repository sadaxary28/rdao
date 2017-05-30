package com.infomaximum.rocksdb.utils;

import com.google.common.primitives.Primitives;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import javassist.util.proxy.ProxyObject;

/**
 * Created by kris on 29.05.17.
 */
public class EqualsUtils {

    public static boolean equals(Object value1, Object value2) {
        if (value1 == null) {
            return (value2 == null);
        } else {
            return value1.equals(value2);
        }
    }

    public static boolean equalsType(Class class1, Class class2) {
        if (class1 == class2) return true;
        if (class1.isPrimitive() && !class2.isPrimitive()) {
            return (class1==Primitives.unwrap(class2));
        } else if (!class1.isPrimitive() && class2.isPrimitive()) {
            return (Primitives.unwrap(class1)==class2);
        } else if (DomainObject.class.isAssignableFrom(class1) && DomainObject.class.isAssignableFrom(class2)) {
            return (getProxySuperClass(class1) == getProxySuperClass(class2));
        }
        return false;
    }

    public static Class<? extends DomainObject> getProxySuperClass(Class<? extends DomainObject> clazz) {
        if (ProxyObject.class.isAssignableFrom(clazz)) {
            return getProxySuperClass((Class<? extends DomainObject>) clazz.getSuperclass());
        } else {
            return clazz;
        }
    }
}
