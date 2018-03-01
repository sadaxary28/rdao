package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.IntervalIndexFilter;
import com.infomaximum.database.utils.key.IntervalIndexKey;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.NotFoundIndexException;
import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.EntityIntervalIndex;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.database.utils.IndexUtils;

import java.util.*;

public class IntervalIndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final List<EntityField> checkedFilterFields;
    private final List<Object> filterValues;
    private final long endValue;

    private KeyValue indexKeyValue;

    public IntervalIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IntervalIndexFilter filter) throws DatabaseException {
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
            this.dataIterator = dataEnumerable.createIterator(structEntity.getColumnFamily());
        }

        this.indexIterator = dataEnumerable.createIterator(entityIndex.columnFamily);
        this.indexKeyValue = indexIterator.seek(IntervalIndexKey.buildKeyPattern(values, filter.getBeginValue()));

        nextImpl();
    }

    @Override
    void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            if (IntervalIndexKey.compare(indexKeyValue.getKey(), endValue) > 0) {
                nextElement = null;
            } else {
                nextElement = findObject(IntervalIndexKey.unpackId(indexKeyValue.getKey()));
            }
            indexKeyValue = indexIterator.step(DBIterator.StepDirection.FORWARD);
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
