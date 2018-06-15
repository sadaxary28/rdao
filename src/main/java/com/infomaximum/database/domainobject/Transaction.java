package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.exception.runtime.ClosedObjectException;
import com.infomaximum.database.provider.*;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.utils.HashIndexUtils;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.RangeIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.utils.key.IndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;
import com.infomaximum.database.utils.key.RangeIndexKey;

import java.io.Serializable;
import java.util.*;

public class Transaction extends DataEnumerable implements AutoCloseable {

    private DBTransaction transaction = null;
    private boolean closed = false;
    private boolean foreignFieldEnabled = true;

    protected Transaction(DBProvider dbProvider) {
        super(dbProvider);
    }

    public boolean isForeignFieldEnabled() {
        return foreignFieldEnabled;
    }

    public void setForeignFieldEnabled(boolean value) {
        this.foreignFieldEnabled = value;
    }

    public DBTransaction getDBTransaction() throws DatabaseException {
        ensureTransaction();
        return transaction;
    }

    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        ensureTransaction();

        try {
            StructEntity entity = Schema.getEntity(clazz);

            long id = transaction.nextId(entity.getColumnFamily());

            T domainObject = buildDomainObject(DomainObject.getConstructor(clazz), id, Collections.emptyList());

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (Field field: entity.getFields()) {
                domainObject.set(field.getName(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T object) throws DatabaseException {
        Map<Field, Serializable> newValues = object.getNewValues();
        if (newValues.isEmpty()) {
            return;
        }

        ensureTransaction();

        final String columnFamily = object.getStructEntity().getColumnFamily();
        final Map<Field, Serializable> loadedValues = object.getLoadedValues();

        // update hash-indexed values
        for (HashIndex index: object.getStructEntity().getHashIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, transaction);
            }
        }

        // update prefix-indexed values
        for (PrefixIndex index: object.getStructEntity().getPrefixIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, transaction);
            }
        }

        // update interval-indexed values
        for (IntervalIndex index: object.getStructEntity().getIntervalIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, transaction);
            }
        }

        // update range-indexed values
        for (RangeIndex index: object.getStructEntity().getRangeIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, transaction);
            }
        }

        // update self-object
        transaction.put(columnFamily, new FieldKey(object.getId()).pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        for (Map.Entry<Field, Serializable> newValue : newValues.entrySet()) {
            Field field = newValue.getKey();
            Object value = newValue.getValue();

            validateUpdatingValue(object, field, value);

            byte[] key = new FieldKey(object.getId(), field.getName()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value, field.getConverter());
                transaction.put(columnFamily, key, bValue);
            } else {
                transaction.delete(columnFamily, key);
            }
        }

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T obj) throws DatabaseException {
        ensureTransaction();

        validateRemovingObject(obj);

        final String columnFamily = obj.getStructEntity().getColumnFamily();
        final Map<Field, Serializable> loadedValues = new HashMap<>();

        // delete hash-indexed values
        for (HashIndex index : obj.getStructEntity().getHashIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete prefix-indexed values
        for (PrefixIndex index: obj.getStructEntity().getPrefixIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete interval-indexed values
        for (IntervalIndex index: obj.getStructEntity().getIntervalIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete interval-indexed values
        for (RangeIndex index: obj.getStructEntity().getRangeIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete self-object
        transaction.deleteRange(columnFamily, FieldKey.buildKeyPrefix(obj.getId()));
    }

    @Override
    public <T, U extends DomainObject> T getValue(final Field field, U obj) throws DatabaseException {
        ensureTransaction();

        byte[] value = transaction.getValue(obj.getStructEntity().getColumnFamily(), new FieldKey(obj.getId(), field.getName()).pack());
        return (T) TypeConvert.unpack(field.getType(), value, field.getConverter());
    }

    @Override
    public DBIterator createIterator(String columnFamily) throws DatabaseException {
        ensureTransaction();

        return transaction.createIterator(columnFamily);
    }

    public void commit() throws DatabaseException {
        if (transaction != null) {
            transaction.commit();
        }
        close();
    }

    @Override
    public void close() throws DatabaseException {
        closed = true;
        try (DBTransaction t = transaction) {
            transaction = null;
        }
    }

    private void ensureTransaction() throws DatabaseException {
        if (closed) {
            throw new ClosedObjectException(this.getClass());
        }

        if (transaction == null) {
            transaction = dbProvider.beginTransaction();
        }
    }

    private void tryLoadFields(String columnFamily, long id, final List<Field> fields, Map<Field, Serializable> loadedValues) throws DatabaseException {
        for (Field field: fields) {
            tryLoadField(columnFamily, id, field, loadedValues);
        }
    }

    private void tryLoadField(String columnFamily, long id, Field field, Map<Field, Serializable> loadedValues) throws DatabaseException {
        if (loadedValues.containsKey(field)) {
            return;
        }

        final byte[] key = new FieldKey(id, field.getName()).pack();
        final byte[] value = transaction.getValue(columnFamily, key);
        loadedValues.put(field, TypeConvert.unpack(field.getType(), value, field.getConverter()));
    }

    static void updateIndexedValue(HashIndex index, long id, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues, DBTransaction transaction) throws DatabaseException {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        // Remove old value-index
        HashIndexUtils.setHashValues(index.sortedFields, prevValues, indexKey.getFieldValues());
        transaction.delete(index.columnFamily, indexKey.pack());

        // Add new value-index
        setHashValues(index.sortedFields, prevValues, newValues, indexKey.getFieldValues());
        transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    static void removeIndexedValue(HashIndex index, long id, Map<Field, Serializable> values, DBTransaction transaction) throws DatabaseException {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        HashIndexUtils.setHashValues(index.sortedFields, values, indexKey.getFieldValues());
        transaction.delete(index.columnFamily, indexKey.pack());
    }

    private void updateIndexedValue(PrefixIndex index, long id, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues, DBTransaction transaction) throws DatabaseException {
        List<String> deletingLexemes = new ArrayList<>();
        List<String> insertingLexemes = new ArrayList<>();
        PrefixIndexUtils.diffIndexedLexemes(index.sortedFields, prevValues, newValues, deletingLexemes, insertingLexemes);

        PrefixIndexUtils.removeIndexedLexemes(index, id, deletingLexemes, transaction);
        PrefixIndexUtils.insertIndexedLexemes(index, id, insertingLexemes, transaction);
    }

    private void removeIndexedValue(PrefixIndex index, long id, Map<Field, Serializable> values, DBTransaction transaction) throws DatabaseException {
        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        for (Field field : index.sortedFields) {
            PrefixIndexUtils.splitIndexingTextIntoLexemes((String) values.get(field), lexemes);
        }

        PrefixIndexUtils.removeIndexedLexemes(index, id, lexemes, transaction);
    }

    static void updateIndexedValue(IntervalIndex index, long id, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final Field indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(id, new long[hashedFields.size()]);

        // Remove old value-index
        HashIndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
        indexKey.setIndexedValue(prevValues.get(indexedField));
        transaction.delete(index.columnFamily, indexKey.pack());

        // Add new value-index
        setHashValues(hashedFields, prevValues, newValues, indexKey.getHashedValues());
        indexKey.setIndexedValue(getValue(indexedField, prevValues, newValues));
        transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    static void removeIndexedValue(IntervalIndex index, long id, Map<Field, Serializable> values, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final IntervalIndexKey indexKey = new IntervalIndexKey(id, new long[hashedFields.size()]);

        HashIndexUtils.setHashValues(hashedFields, values, indexKey.getHashedValues());
        indexKey.setIndexedValue(values.get(index.getIndexedField()));

        transaction.delete(index.columnFamily, indexKey.pack());
    }

    static void updateIndexedValue(RangeIndex index, long id, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(id, new long[hashedFields.size()]);

        // Remove old value-index
        HashIndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
        RangeIndexUtils.removeIndexedRange(index, indexKey,
                prevValues.get(index.getBeginIndexedField()),
                prevValues.get(index.getEndIndexedField()),
                transaction);

        // Add new value-index
        setHashValues(hashedFields, prevValues, newValues, indexKey.getHashedValues());
        RangeIndexUtils.insertIndexedRange(index, indexKey,
                getValue(index.getBeginIndexedField(), prevValues, newValues),
                getValue(index.getEndIndexedField(), prevValues, newValues),
                transaction);
    }

    static void removeIndexedValue(RangeIndex index, long id, Map<Field, Serializable> values, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(id, new long[hashedFields.size()]);

        HashIndexUtils.setHashValues(hashedFields, values, indexKey.getHashedValues());
        RangeIndexUtils.removeIndexedRange(index, indexKey,
                values.get(index.getBeginIndexedField()),
                values.get(index.getEndIndexedField()),
                transaction);
    }

    private static void setHashValues(List<Field> fields, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues, long[] destination) {
        for (int i = 0; i < fields.size(); ++i) {
            Field field = fields.get(i);
            Object value = getValue(field, prevValues, newValues);
            destination[i] = HashIndexUtils.buildHash(field.getType(), value, field.getConverter());
        }
    }

    private static Object getValue(Field field, Map<Field, Serializable> prevValues, Map<Field, Serializable> newValues) {
        return newValues.containsKey(field) ? newValues.get(field) : prevValues.get(field);
    }

    private static boolean anyChanged(List<Field> fields, Map<Field, Serializable> newValues) {
        for (Field iField: fields) {
            if (newValues.containsKey(iField)) {
                return true;
            }
        }
        return false;
    }

    private void validateUpdatingValue(DomainObject obj, Field field, Object value) throws DatabaseException {
        if (value == null) {
            return;
        }

        if (!foreignFieldEnabled || !field.isForeign()) {
            return;
        }

        long fkeyIdValue = (Long) value;
        if (transaction.getValue(field.getForeignDependency().getColumnFamily(), new FieldKey(fkeyIdValue).pack()) == null) {
            throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), field, fkeyIdValue);
        }
    }

    private void validateRemovingObject(DomainObject obj) throws DatabaseException {
        if (!foreignFieldEnabled) {
            return;
        }

        List<StructEntity.Reference> references = obj.getStructEntity().getReferencingForeignFields();
        if (references.isEmpty()) {
            return;
        }

        KeyPattern keyPattern = IndexKey.buildKeyPattern(obj.getId());
        for (StructEntity.Reference ref : references) {
            try (DBIterator iterator = transaction.createIterator(ref.fieldIndex.columnFamily)) {
                KeyValue keyValue = iterator.seek(keyPattern);
                if (keyValue != null) {
                    long referencingId = IndexKey.unpackId(keyValue.getKey());
                    throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), referencingId, ref.objClass);
                }
            }
        }
    }
}
