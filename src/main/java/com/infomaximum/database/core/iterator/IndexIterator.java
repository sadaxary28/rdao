package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.NotFoundIndexException;

import java.util.*;

public class IndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final EntityIndex entityIndex;
    private final List<EntityField> checkedFilterFields;
    private final List<Object> filterValues;

    public IndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IndexFilter filter) throws DataSourceDatabaseException {
        super(dataEnumerable, clazz);

        StructEntity structEntity = Schema.getEntity(clazz);
        Map<String, Object> filters = filter.getValues();
        this.entityIndex = structEntity.getIndex(filters.keySet());

        checkIndex(filters);

        List<EntityField> filterFields = null;
        List<Object> filterValues = null;

        long[] values = new long[filters.size()];
        for (int i = 0; i < entityIndex.sortedFields.size(); ++i) {
            EntityField field = entityIndex.sortedFields.get(i);
            Object value = filters.get(field.getName());
            values[i] = IndexUtils.buildHash(field.getType(), value);
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
            this.dataIteratorId = dataEnumerable.createIterator(structEntity.getColumnFamily(), null);
        }

        this.indexIteratorId = dataEnumerable.createIterator(entityIndex.columnFamily, IndexKey.buildKeyPattern(values));

        nextImpl();
    }

    private void checkIndex(final Map<String, Object> filters) {
        if (entityIndex == null) {
            throw new NotFoundIndexException(clazz, filters.keySet());
        }

        for (EntityField field : entityIndex.sortedFields) {
            Object filterValue = filters.get(field.getName());
            if (filterValue != null) {
                field.throwIfNotMatch(filterValue.getClass());
            }
        }
    }

    @Override
    void nextImpl() throws DataSourceDatabaseException {
        while (true) {
            KeyValue keyValue = dataEnumerable.next(indexIteratorId);
            if (keyValue == null) {
                break;
            }

            nextElement = findObject(IndexKey.unpackId(keyValue.getKey()));
            if (nextElement != null) {
                return;
            }
        }

        nextElement = null;
        close();
    }

    @Override
    boolean checkFilter(E obj) throws DataSourceDatabaseException {
        for (int i = 0; i < checkedFilterFields.size(); ++i) {
            EntityField field = checkedFilterFields.get(i);
            if (!IndexUtils.equals(field.getType(), filterValues.get(i), obj.get(field.getType(), field.getName()))) {
                return false;
            }
        }

        return true;
    }
}
