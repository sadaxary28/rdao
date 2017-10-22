package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.IllegalTypeException;
import com.infomaximum.database.utils.TypeConvert;

import java.lang.reflect.Constructor;
import java.util.Collection;

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

    public static <T extends DomainObject> T buildDomainObject(final Class<T> clazz, long id, Collection<String> preInitializedFields, DataEnumerable dataSource) {
        T obj = buildDomainObject(clazz, id, dataSource);
        for (String field : preInitializedFields) {
            obj._setLoadedField(field, null);
        }
        return obj;
    }

    private static <T extends DomainObject> T buildDomainObject(final Class<T> clazz, long id, DataEnumerable dataSource) {
        try {
            Constructor<T> constructor = clazz.getConstructor(long.class);

            T domainObject = constructor.newInstance(id);

            //Устанавливаем dataSource
            StructEntity.dataSourceField.set(domainObject, dataSource);

            return domainObject;
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeException(e);
        }
    }

    public static <T extends DomainObject> T nextObject(final Class<T> clazz, Collection<String> preInitializedFields
            , DataEnumerable dataEnumerable
            , long iteratorId
            , NextState state) throws DataSourceDatabaseException {
        T obj = null;
        if (state != null && state.isEmpty()) {
            obj = buildDomainObject(clazz, state.nextId, preInitializedFields, dataEnumerable);
            state.clear();
        }

        while (true) {
            KeyValue keyValue = dataEnumerable.next(iteratorId);
            if (keyValue == null) {
                break;
            }

            FieldKey key = FieldKey.unpack(keyValue.getKey());
            if (key.isBeginningObject()) {
                if (obj == null) {
                    obj = buildDomainObject(clazz, key.getId(), preInitializedFields, dataEnumerable);
                    continue;
                }

                if (state != null) {
                    state.nextId = key.getId();
                }
                break;
            } else {
                EntityField field = obj.getStructEntity().getField(key.getFieldName());

                obj._setLoadedField(key.getFieldName(), TypeConvert.unpack(field.getType(), keyValue.getValue(), field.getPacker()));
            }
        }

        return obj;
    }
}
