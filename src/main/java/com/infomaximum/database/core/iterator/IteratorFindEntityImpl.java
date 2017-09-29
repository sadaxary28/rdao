package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.runtime.NotFoundIndexDatabaseException;
import com.infomaximum.database.utils.EqualsUtils;

import java.util.*;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorFindEntityImpl<E extends DomainObject> implements IteratorEntity<E> {

    private final DataSource dataSource;
    private final DataEnumerable dataEnumerable;
    private final Class<E> clazz;
    private final StructEntityIndex structEntityIndex;
    private final long indexIteratorId;
    private final List<Field> checkedFilterFields;
    private final List<Object> filterValues;
    private final KeyPattern dataKeyPattern;
    private final long dataIteratorId;

    private E nextElement;

    public IteratorFindEntityImpl(DataSource dataSource, DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, Map<String, Object> filters, long transactionId) throws DataSourceDatabaseException {
        this.dataSource = dataSource;
        this.dataEnumerable = dataEnumerable;
        this.clazz = clazz;
        StructEntity structEntity = StructEntity.getInstance(clazz);
        this.structEntityIndex = structEntity.getStructEntityIndex(filters.keySet());

        checkIndex(filters);

        List<Field> filterFields = null;
        List<Object> filterValues = null;

        long[] values = new long[filters.size()];
        for (int i = 0; i < structEntityIndex.sortedFields.size(); ++i) {
            Field field = structEntityIndex.sortedFields.get(i);
            Object value = filters.get(field.name());
            values[i] = IndexUtils.buildHash(field.type(), value);
            if (IndexUtils.toLongCastable(field.type())) {
                continue;
            }

            if (filterFields == null) {
                filterFields = new ArrayList<>();
                filterValues = new ArrayList<>();
            }

            filterFields.add(field);
            filterValues.add(value);
        }

        this.checkedFilterFields = filterFields;
        this.filterValues = filterValues;
        this.dataKeyPattern = buildDataKeyPattern(filterFields, loadingFields);

        final KeyPattern indexKeyPattern = IndexKey.buildKeyPattern(values);
        if (transactionId == -1) {
            this.indexIteratorId = dataSource.createIterator(structEntityIndex.columnFamily, indexKeyPattern);
            this.dataIteratorId = dataKeyPattern != null ? dataSource.createIterator(structEntity.annotationEntity.name(), null) : -1;
        } else {
            this.indexIteratorId = dataSource.createIterator(structEntityIndex.columnFamily, indexKeyPattern, transactionId);
            this.dataIteratorId = dataKeyPattern != null ? dataSource.createIterator(structEntity.annotationEntity.name(), null, transactionId) : -1;
        }

        nextImpl();
    }

    private KeyPattern buildDataKeyPattern(List<Field> fields1, Set<String> fields2) {
        if (fields2 == null) {
            fields2 = Collections.emptySet();
        }

        if (fields1 == null || fields1.isEmpty()) {
            return fields2.isEmpty() ? null : FieldKey.buildKeyPattern(fields2);
        }

        Set<String> fields = new HashSet<>(fields1.size() + fields2.size());
        fields1.forEach(field -> fields.add(field.name()));
        fields.addAll(fields2);
        return FieldKey.buildKeyPattern(fields);
    }

    @Override
    public boolean hasNext() {
        return nextElement != null;
    }

    @Override
    public E next() throws DataSourceDatabaseException {
        if (nextElement == null) {
            throw new NoSuchElementException();
        }

        E element = nextElement;
        nextImpl();
        return element;
    }

    @Override
    public void close() throws DataSourceDatabaseException {
        dataSource.closeIterator(indexIteratorId);
        if (dataIteratorId != -1) {
            dataSource.closeIterator(dataIteratorId);
        }
    }

    private void nextImpl() throws DataSourceDatabaseException {
        while (true) {
            KeyValue keyValue = dataSource.next(indexIteratorId);
            if (keyValue == null) {
                break;
            }

            nextElement = findObject(IndexKey.unpack(keyValue.getKey()));
            if (nextElement != null) {
                return;
            }
        }

        nextElement = null;
        close();
    }

    private void checkIndex(final Map<String, Object> filters) {
        if (structEntityIndex == null) {
            throw new NotFoundIndexDatabaseException(clazz, filters.keySet());
        }

        for (Field field : structEntityIndex.sortedFields) {
            Object filterValue = filters.get(field.name());
            if (filterValue != null && !EqualsUtils.equalsType(field.type(), filterValue.getClass())) {
                throw new RuntimeException("Not equals type field " + field.type() + " and type value " + filterValue.getClass());
            }
        }
    }

    private E findObject(IndexKey key) throws DataSourceDatabaseException {
        if (dataKeyPattern == null) {
            return DomainObjectUtils.buildDomainObject(clazz, key.getId(), dataEnumerable);
        }

        dataKeyPattern.setPrefix(FieldKey.buildKeyPrefix(key.getId()));
        dataSource.seekIterator(dataIteratorId, dataKeyPattern);

        E obj = DomainObjectUtils.nextObject(clazz, dataSource, dataIteratorId, dataEnumerable, null);
        return checkFilter(obj) ? obj : null;
    }

    private boolean checkFilter(E obj) throws DataSourceDatabaseException {
        if (checkedFilterFields == null) {
            return true;
        }
        for (int i = 0; i < checkedFilterFields.size(); ++i) {
            Field field = checkedFilterFields.get(i);
            if (!IndexUtils.equals(field.type(), filterValues.get(i), obj.get(field.type(), field.name()))) {
                return false;
            }
        }

        return true;
    }
}
