package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.database.schema.EntityIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.utils.key.IndexKey;
import com.infomaximum.database.exception.runtime.NotFoundIndexException;

import java.util.*;

public class IndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final List<EntityField> checkedFilterFields;
    private final List<Object> filterValues;

    private KeyValue indexKeyValue;

    public IndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IndexFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields);

        StructEntity structEntity = Schema.getEntity(clazz);
        Map<String, Object> filters = filter.getValues();
        final EntityIndex entityIndex = structEntity.getIndex(filters.keySet());
        if (entityIndex == null) {
            throw new NotFoundIndexException(clazz, filters.keySet());
        }

        List<EntityField> filterFields = null;
        List<Object> filterValues = null;

        long[] values = new long[entityIndex.sortedFields.size()];
        for (int i = 0; i < entityIndex.sortedFields.size(); ++i) {
            EntityField field = entityIndex.sortedFields.get(i);
            Object value = filters.get(field.getName());
            if (value != null) {
                field.throwIfNotMatch(value.getClass());
            }

            values[i] = IndexUtils.buildHash(field.getType(), value, field.getConverter());
            if (IndexUtils.toLongCastable(field.getType())) {
                continue;
            }

            if (filterFields == null) {
                filterFields = new ArrayList<>();
                filterValues = new ArrayList<>();
            }

            filterFields.add(field);
            filterValues.add(value);
        }

        this.checkedFilterFields = filterFields != null ? filterFields : Collections.emptyList();
        this.filterValues = filterValues;

        this.dataKeyPattern = buildDataKeyPattern(filterFields, loadingFields);
        if (this.dataKeyPattern != null) {
            this.dataIterator = dataEnumerable.createIterator(structEntity.getColumnFamily());
        }

        this.indexIterator = dataEnumerable.createIterator(entityIndex.columnFamily);
        this.indexKeyValue = indexIterator.seek(IndexKey.buildKeyPattern(values));

        nextImpl();
    }

    @Override
    void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            nextElement = findObject(IndexKey.unpackId(indexKeyValue.getKey()));
            indexKeyValue = indexIterator.next();
            if (nextElement != null) {
                return;
            }
        }

        nextElement = null;
        close();
    }

    @Override
    boolean checkFilter(E obj) throws DatabaseException {
        for (int i = 0; i < checkedFilterFields.size(); ++i) {
            EntityField field = checkedFilterFields.get(i);
            if (!IndexUtils.equals(field.getType(), filterValues.get(i), obj.get(field.getType(), field.getName()))) {
                return false;
            }
        }

        return true;
    }
}
