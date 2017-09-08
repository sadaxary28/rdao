package com.infomaximum.rocksdb.core.objectsource.proxy;

import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.domainobject.DomainObject;
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
    public boolean isHandled(Method method) {
        StructEntity structEntity = HashStructEntities.getStructEntity(clazz);

//        if ("save".equals(method.getName())) {
//            //Вот save мы ловим ставим свой обработчик
//            return true;
//        } else if ("remove".equals(method.getName())) {
//            //Вот remove мы ловим ставим свой обработчик
//            return true;
//        } else if (structEntity.isGetterMethod(method.getName())) {
//            //Ловим getter'ы для lazy полей
//            return true;
//        } else if (structEntity.isSetterMethod(method.getName())) {
//            //Ловим setter'ы для lazy полей
//            return true;
//        } else {
            return false;
//        }
    }
}
