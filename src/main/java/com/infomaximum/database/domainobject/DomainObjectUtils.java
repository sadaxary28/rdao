package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.FieldNotFoundDatabaseException;
import com.infomaximum.database.exeption.runtime.IllegalTypeDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Created by kris on 28.04.17.
 */
public class DomainObjectUtils {

    public static class NextState {

        protected long nextId = -1;

        protected boolean isEmpty() {
            return nextId != -1;
        }
        protected void clear() {
            nextId = -1;
        }
    }

    public static <T extends DomainObject> T buildDomainObject(final Class<T> clazz, long id, DataSource dataSource) {
        try {
            Constructor<T> constructor = clazz.getConstructor(long.class);

            T domainObject = constructor.newInstance(id);

            //Устанавливаем dataSource
            HashStructEntities.dataSourceField.set(domainObject, dataSource);

            return domainObject;
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeDatabaseException(e);
        }
    }

    public static <T extends DomainObject> T nextObject(final Class<T> clazz, DataSource dataSource, long iteratorId, NextState state) throws DataSourceDatabaseException {
        T obj = null;
        if (state != null && state.isEmpty()) {
            obj = buildDomainObject(clazz, state.nextId, dataSource);
            state.clear();
        }

        while (true) {
            KeyValue keyValue = dataSource.next(iteratorId);
            if (keyValue == null) {
                break;
            }

            FieldKey key = FieldKey.unpack(keyValue.getKey());
            if (key.isBeginningObject()) {
                if (obj == null) {
                    obj = buildDomainObject(clazz, key.getId(), dataSource);
                    continue;
                }

                if (state != null) {
                    state.nextId = key.getId();
                }
                break;
            } else {
                Field field = obj.getStructEntity().getFieldByName(key.getFieldName());
                if (field == null) {
                    throw new FieldNotFoundDatabaseException(clazz, key.getFieldName());
                }

                obj.setLoadedField(key.getFieldName(), TypeConvert.get(field.type(), keyValue.getValue()));
            }
        }

        return obj;
    }
}
