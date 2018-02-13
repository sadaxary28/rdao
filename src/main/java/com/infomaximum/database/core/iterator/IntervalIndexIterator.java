package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.*;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.IntervalIndexFilter;
import com.infomaximum.database.domainobject.key.IntervalIndexKey;
import com.infomaximum.database.exception.DataSourceDatabaseException;
import com.infomaximum.database.exception.runtime.NotFoundIndexException;
import com.infomaximum.database.utils.IndexUtils;

import java.util.*;

public class IntervalIndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final List<EntityField> checkedFilterFields;
    private final List<Object> filterValues;
    private final long endValue;

    private KeyValue indexKeyValue;

    public IntervalIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IntervalIndexFilter filter) throws DataSourceDatabaseException {
        super(dataEnumerable, clazz, loadingFields);

        StructEntity structEntity = Schema.getEntity(clazz);
        Map<String, Object> filters = filter.getHashedValues();
        EntityIntervalIndex entityIndex = structEntity.getIntervalIndex(filters.keySet(), filter.getIndexedFieldName());
        if (entityIndex == null) {
            throw new NotFoundIndexException(clazz, filters.keySet());
        }

        List<EntityField> filterFields = null;
        List<Object> filterValues = null;

        final List<EntityField> hashedFields = entityIndex.getHashedFields();
        long[] values = new long[hashedFields.size()];
        for (int i = 0; i < hashedFields.size(); ++i) {
            EntityField field = hashedFields.get(i);
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

        EntityField field = entityIndex.getIndexedField();
        field.throwIfNotMatch(filter.getBeginValue().getClass());
        field.throwIfNotMatch(filter.getEndValue().getClass());

        this.checkedFilterFields = filterFields != null ? filterFields : Collections.emptyList();
        this.filterValues = filterValues;
        this.endValue = IntervalIndexKey.castToLong(filter.getEndValue());

        this.dataKeyPattern = buildDataKeyPattern(filterFields, loadingFields);
        if (this.dataKeyPattern != null) {
            this.dataIteratorId = dataEnumerable.createIterator(structEntity.getColumnFamily());
        }

        this.indexIteratorId = dataEnumerable.createIterator(entityIndex.columnFamily);
        this.indexKeyValue = dataEnumerable.seek(indexIteratorId, IntervalIndexKey.buildKeyPattern(values, filter.getBeginValue()));

        nextImpl();
    }

    @Override
    void nextImpl() throws DataSourceDatabaseException {
        while (indexKeyValue != null) {
            if (IntervalIndexKey.compare(indexKeyValue.getKey(), endValue) > 0) {
                nextElement = null;
            } else {
                nextElement = findObject(IntervalIndexKey.unpackId(indexKeyValue.getKey()));
            }
            indexKeyValue = dataEnumerable.step(indexIteratorId, DataSource.StepDirection.FORWARD);
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
