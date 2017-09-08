package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.entitysource.EntitySource;
import com.infomaximum.database.exeption.ReflectionDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Created by kris on 28.04.17.
 */
public class DomainObjectUtils {

    public static <T extends DomainObject> T buildDomainObject(DataSource dataSource, final Class<T> clazz, EntitySource entitySource) throws ReflectionDatabaseException {
        try {
            Constructor<T> constructor = clazz.getConstructor(long.class);

            T domainObject = constructor.newInstance(entitySource.getId());

            //Устанавливаем dataSource
            HashStructEntities.getDataSourceField().set(domainObject, dataSource);

            //Загружаем поля
            Map<String, byte[]> data = entitySource.getFields();
            if (data!=null) {
                StructEntity structEntity = domainObject.getStructEntity();

                for (Field field: structEntity.getFields()) {
                    String fieldName = field.name();
                    if (data.containsKey(fieldName)) {
                        byte[] bValue = data.get(fieldName);
                        Object value = TypeConvert.get(field.type(), bValue);
                        domainObject.set(fieldName, value);
                    }
                }
            }

            return domainObject;
        } catch (ReflectiveOperationException e) {
            throw new ReflectionDatabaseException(e);
        }
    }

}
