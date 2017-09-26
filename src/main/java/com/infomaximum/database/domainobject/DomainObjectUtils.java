package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.ReflectionDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    public static EntitySource nextEntitySource(DataSource dataSource, long iteratorId, final Set<String> fields, EntitySource[] state) throws DataSourceDatabaseException {
        EntitySource entitySource = null;
        if (state != null) {
            entitySource = state[0];
            state[0] = null;
        }

        while (true) {
            KeyValue keyValue = dataSource.next(iteratorId);
            if (keyValue == null) {
                break;
            }

            FieldKey key = FieldKey.unpack(keyValue.getKey());
            if (key.isBeginningObject()) {
                if (entitySource == null) {
                    entitySource = new EntitySource(key.getId(), new HashMap<>());
                    continue;
                }

                if (state != null) {
                    state[0] = new EntitySource(key.getId(), new HashMap<>());
                }
                break;
            } else {
                FieldKey fieldKey = (FieldKey)key;
                if (fields.contains(fieldKey.getFieldName())) {
                    entitySource.getFields().put(fieldKey.getFieldName(), keyValue.getValue());
                }
            }
        }

        return entitySource;
    }
}
