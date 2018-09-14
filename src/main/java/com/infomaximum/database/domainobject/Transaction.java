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
            domainObject._setAsJustCreated();

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (Field field: entity.getFields()) {
                domainObject.set(field.getNumber(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T object) throws DatabaseException {
        Value<Serializable>[] newValues = object.getNewValues();
        if (newValues == null) {
            return;
        }

        ensureTransaction();

        final String columnFamily = object.getStructEntity().getColumnFamily();
        final Value<Serializable>[] loadedValues = object.getLoadedValues();

        // update hash-indexed values
        for (HashIndex index: object.getStructEntity().getHashIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object, index.sortedFields, loadedValues);
                updateIndexedValue(index, object, loadedValues, newValues, transaction);
            }
        }

        // update prefix-indexed values
        for (PrefixIndex index: object.getStructEntity().getPrefixIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object, index.sortedFields, loadedValues);
                updateIndexedValue(index, object, loadedValues, newValues, transaction);
            }
        }

        // update interval-indexed values
        for (IntervalIndex index: object.getStructEntity().getIntervalIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object, index.sortedFields, loadedValues);
                updateIndexedValue(index, object, loadedValues, newValues, transaction);
            }
        }

        // update range-indexed values
        for (RangeIndex index: object.getStructEntity().getRangeIndexes()) {
            if (anyChanged(index.sortedFields, newValues)) {
                tryLoadFields(columnFamily, object, index.sortedFields, loadedValues);
                updateIndexedValue(index, object, loadedValues, newValues, transaction);
            }
        }

        // update self-object
        if (object._isJustCreated()) {
            transaction.put(columnFamily, new FieldKey(object.getId()).pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        }
        for (int i = 0; i < newValues.length; ++i) {
            Value<Serializable> newValue = newValues[i];
            if (newValue == null) {
                continue;
            }

            Field field = object.getStructEntity().getFields()[i];
            Object value = newValue.getValue();

            validateUpdatingValue(object, field, value);

            byte[] key = new FieldKey(object.getId(), field.getNameBytes()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value, field.getConverter());
                transaction.put(columnFamily, key, bValue);
            } else if (!object._isJustCreated()) {
                transaction.delete(columnFamily, key);
            }
        }

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T obj) throws DatabaseException {
        ensureTransaction();

        validateForeignValues(obj);

        final String columnFamily = obj.getStructEntity().getColumnFamily();
        final Value<Serializable>[] loadedValues = new Value[obj.getStructEntity().getFields().length];

        // delete hash-indexed values
        for (HashIndex index : obj.getStructEntity().getHashIndexes()) {
            tryLoadFields(columnFamily, obj, index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete prefix-indexed values
        for (PrefixIndex index: obj.getStructEntity().getPrefixIndexes()) {
            tryLoadFields(columnFamily, obj, index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete interval-indexed values
        for (IntervalIndex index: obj.getStructEntity().getIntervalIndexes()) {
            tryLoadFields(columnFamily, obj, index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete range-indexed values
        for (RangeIndex index: obj.getStructEntity().getRangeIndexes()) {
            tryLoadFields(columnFamily, obj, index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, transaction);
        }

        // delete self-object
        transaction.deleteRange(columnFamily, FieldKey.buildKeyPrefix(obj.getId()));
    }

    /**
     * The method is not transactional
     */
    public <T extends DomainObject & DomainObjectEditable> void clearUnsafe(Class<T> objClass) throws DatabaseException {
        ensureTransaction();

        StructEntity entity = Schema.getEntity(objClass);

        validateForeignValues(entity);

        // delete hash-indexed values
        for (HashIndex index : entity.getHashIndexes()) {
            recreateColumnFamily(index.columnFamily);
        }

        // delete prefix-indexed values
        for (PrefixIndex index: entity.getPrefixIndexes()) {
            recreateColumnFamily(index.columnFamily);
        }

        // delete interval-indexed values
        for (IntervalIndex index: entity.getIntervalIndexes()) {
            recreateColumnFamily(index.columnFamily);
        }

        // delete range-indexed values
        for (RangeIndex index: entity.getRangeIndexes()) {
            recreateColumnFamily(index.columnFamily);
        }

        // delete objects
        recreateColumnFamily(entity.getColumnFamily());
    }

    private void recreateColumnFamily(String columnFamily) throws DatabaseException {
        getDbProvider().dropColumnFamily(columnFamily);
        getDbProvider().createColumnFamily(columnFamily);
    }

    @Override
    public <T, U extends DomainObject> T getValue(final Field field, U obj) throws DatabaseException {
        ensureTransaction();

        byte[] value = transaction.getValue(obj.getStructEntity().getColumnFamily(), new FieldKey(obj.getId(), field.getNameBytes()).pack());
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

    private void tryLoadFields(String columnFamily, DomainObject obj, List<Field> fields, Value<Serializable>[] loadedValues) throws DatabaseException {
        if (obj._isJustCreated()) {
            return;
        }

        for (Field field: fields) {
            tryLoadField(columnFamily, obj.getId(), field, loadedValues);
        }
    }

    private void tryLoadField(String columnFamily, long id, Field field, Value<Serializable>[] loadedValues) throws DatabaseException {
        if (loadedValues[field.getNumber()] != null) {
            return;
        }

        final byte[] key = new FieldKey(id, field.getNameBytes()).pack();
        final byte[] value = transaction.getValue(columnFamily, key);
        loadedValues[field.getNumber()] = Value.of(TypeConvert.unpack(field.getType(), value, field.getConverter()));
    }

    private static void updateIndexedValue(HashIndex index, DomainObject obj, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, DBTransaction transaction) throws DatabaseException {
        final IndexKey indexKey = new IndexKey(obj.getId(), new long[index.sortedFields.size()]);

        if (!obj._isJustCreated()) {
            // Remove old value-index
            HashIndexUtils.setHashValues(index.sortedFields, prevValues, indexKey.getFieldValues());
            transaction.delete(index.columnFamily, indexKey.pack());
        }

        // Add new value-index
        setHashValues(index.sortedFields, prevValues, newValues, indexKey.getFieldValues());
        transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    private static void removeIndexedValue(HashIndex index, long id, Value<Serializable>[] values, DBTransaction transaction) throws DatabaseException {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        HashIndexUtils.setHashValues(index.sortedFields, values, indexKey.getFieldValues());
        transaction.delete(index.columnFamily, indexKey.pack());
    }

    private static void updateIndexedValue(PrefixIndex index, DomainObject obj, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, DBTransaction transaction) throws DatabaseException {
        List<String> deletingLexemes = new ArrayList<>();
        List<String> insertingLexemes = new ArrayList<>();
        PrefixIndexUtils.diffIndexedLexemes(index.sortedFields, prevValues, newValues, deletingLexemes, insertingLexemes);

        if (!obj._isJustCreated()) {
            PrefixIndexUtils.removeIndexedLexemes(index, obj.getId(), deletingLexemes, transaction);
        }
        PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), insertingLexemes, transaction);
    }

    private static void removeIndexedValue(PrefixIndex index, long id, Value<Serializable>[] values, DBTransaction transaction) throws DatabaseException {
        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        for (Field field : index.sortedFields) {
            PrefixIndexUtils.splitIndexingTextIntoLexemes((String) values[field.getNumber()].getValue(), lexemes);
        }

        PrefixIndexUtils.removeIndexedLexemes(index, id, lexemes, transaction);
    }

    private static void updateIndexedValue(IntervalIndex index, DomainObject obj, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final Field indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(obj.getId(), new long[hashedFields.size()]);

        if (!obj._isJustCreated()) {
            // Remove old value-index
            HashIndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
            indexKey.setIndexedValue(prevValues[indexedField.getNumber()].getValue());
            transaction.delete(index.columnFamily, indexKey.pack());
        }

        // Add new value-index
        setHashValues(hashedFields, prevValues, newValues, indexKey.getHashedValues());
        indexKey.setIndexedValue(getValue(indexedField, prevValues, newValues));
        transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    private static void removeIndexedValue(IntervalIndex index, long id, Value<Serializable>[] values, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final IntervalIndexKey indexKey = new IntervalIndexKey(id, new long[hashedFields.size()]);

        HashIndexUtils.setHashValues(hashedFields, values, indexKey.getHashedValues());
        indexKey.setIndexedValue(values[index.getIndexedField().getNumber()].getValue());

        transaction.delete(index.columnFamily, indexKey.pack());
    }

    private static void updateIndexedValue(RangeIndex index, DomainObject obj, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(obj.getId(), new long[hashedFields.size()]);

        if (!obj._isJustCreated()) {
            // Remove old value-index
            HashIndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
            RangeIndexUtils.removeIndexedRange(index, indexKey,
                    prevValues[index.getBeginIndexedField().getNumber()].getValue(),
                    prevValues[index.getEndIndexedField().getNumber()].getValue(),
                    transaction);
        }

        // Add new value-index
        setHashValues(hashedFields, prevValues, newValues, indexKey.getHashedValues());
        RangeIndexUtils.insertIndexedRange(index, indexKey,
                getValue(index.getBeginIndexedField(), prevValues, newValues),
                getValue(index.getEndIndexedField(), prevValues, newValues),
                transaction);
    }

    private static void removeIndexedValue(RangeIndex index, long id, Value<Serializable>[] values, DBTransaction transaction) throws DatabaseException {
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(id, new long[hashedFields.size()]);

        HashIndexUtils.setHashValues(hashedFields, values, indexKey.getHashedValues());
        RangeIndexUtils.removeIndexedRange(index, indexKey,
                values[index.getBeginIndexedField().getNumber()].getValue(),
                values[index.getEndIndexedField().getNumber()].getValue(),
                transaction);
    }

    private static void setHashValues(List<Field> fields, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, long[] destination) {
        for (int i = 0; i < fields.size(); ++i) {
            Field field = fields.get(i);
            Object value = getValue(field, prevValues, newValues);
            destination[i] = HashIndexUtils.buildHash(field.getType(), value, field.getConverter());
        }
    }

    private static Object getValue(Field field, Value<Serializable>[] prevValues, Value<Serializable>[] newValues) {
        Value<Serializable> value = newValues[field.getNumber()];
        if (value == null) {
            value = prevValues[field.getNumber()];
        }
        return value.getValue();
    }

    private static boolean anyChanged(List<Field> fields, Value<Serializable>[] newValues) {
        for (Field field: fields) {
            if (newValues[field.getNumber()] != null) {
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

    private void validateForeignValues(DomainObject obj) throws DatabaseException {
        if (!foreignFieldEnabled) {
            return;
        }

        List<StructEntity.Reference> references = obj.getStructEntity().getReferencingForeignFields();
        if (references.isEmpty()) {
            return;
        }

        KeyPattern keyPattern = IndexKey.buildKeyPattern(obj.getId());
        for (StructEntity.Reference ref : references) {
            try (DBIterator i = transaction.createIterator(ref.fieldIndex.columnFamily)) {
                KeyValue keyValue = i.seek(keyPattern);
                if (keyValue != null) {
                    long referencingId = IndexKey.unpackId(keyValue.getKey());
                    throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), referencingId, ref.objClass);
                }
            }
        }
    }

    private void validateForeignValues(StructEntity entity) throws DatabaseException {
        if (!foreignFieldEnabled) {
            return;
        }

        List<StructEntity.Reference> references = entity.getReferencingForeignFields();
        if (references.isEmpty()) {
            return;
        }

        KeyPattern keyPattern = new KeyPattern((byte[]) null);
        keyPattern.setForBackward(true);
        for (StructEntity.Reference ref : references) {
            if (ref.objClass.equals(entity.getObjectClass())) {
                continue;
            }

            try (DBIterator i = transaction.createIterator(ref.fieldIndex.columnFamily)) {
                KeyValue keyValue = i.seek(keyPattern);
                if (keyValue != null && IndexKey.unpackFirstIndexedValue(keyValue.getKey()) != 0) {
                    long referencingId = IndexKey.unpackId(keyValue.getKey());
                    long objId = IndexKey.unpackFirstIndexedValue(keyValue.getKey());
                    throw new ForeignDependencyException(objId, entity.getObjectClass(), referencingId, ref.objClass);
                }
            }
        }
    }
}
