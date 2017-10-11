package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.schema.*;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.ForeignDependencyException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.util.*;

public class Transaction extends DataEnumerable implements AutoCloseable {

    private long transactionId = -1;
    private boolean foreignKeyEnabled = true;

    protected Transaction(DataSource dataSource) {
        super(dataSource);
    }

    public void setForeignKeyEnabled(boolean value) {
        this.foreignKeyEnabled = value;
    }

    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        try {
            StructEntity entity = Schema.getEntity(clazz);

            long id = dataSource.nextId(entity.getColumnFamily());

            T domainObject = DomainObjectUtils.buildDomainObject(clazz, id, this);

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (EntityField field: entity.getFields()) {
                domainObject.set(field.getName(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T object) throws DatabaseException {
        ensureTransaction();

        Map<EntityField, Object> newValues = object.getNewValues();

        final String columnFamily = object.getStructEntity().getColumnFamily();
        final List<Modifier> modifiers = new ArrayList<>();

        // update indexed values
        Map<EntityField, Object> loadedValues = object.getLoadedValues();
        for (EntityIndex index : object.getStructEntity().getIndexes()){
            if (isChangedIndex(index, newValues)) {
                tryLoadFields(columnFamily, object.getId(), index.sortedFields, loadedValues);
                updateIndexedValue(index, object.getId(), loadedValues, newValues, modifiers);
            }
        }

        // update prefix-indexed values
        for (EntityPrefixIndex index: object.getStructEntity().getPrefixIndexes()) {
            if (newValues.containsKey(index.field)) {
                tryLoadField(columnFamily, object.getId(), index.field, loadedValues);
                updateIndexedValue(index, object.getId(), (String) loadedValues.get(index.field), (String) newValues.get(index.field), modifiers);
            }
        }

        // update self-object
        modifiers.add(new ModifierSet(columnFamily, new FieldKey(object.getId()).pack()));
        for (Map.Entry<EntityField, Object> newValue: newValues.entrySet()) {
            EntityField field = newValue.getKey();
            Object value = newValue.getValue();

            validateUpdatingValue(object, field, value);

            byte[] key = new FieldKey(object.getId(), field.getName()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value, field.getPacker());
                modifiers.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                modifiers.add(new ModifierRemove(columnFamily, key, false));
            }
        }

        dataSource.modify(modifiers, transactionId);

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T obj) throws DatabaseException {
        ensureTransaction();

        validateRemovingObject(obj);

        final String columnFamily = obj.getStructEntity().getColumnFamily();
        final List<Modifier> modifiers = new ArrayList<>();

        // delete indexed values
        Map<EntityField, Object> loadedValues = new HashMap<>();
        for (EntityIndex index : obj.getStructEntity().getIndexes()) {
            tryLoadFields(columnFamily, obj.getId(), index.sortedFields, loadedValues);
            removeIndexedValue(index, obj.getId(), loadedValues, modifiers);
        }

        // delete prefix-indexed values
        for (EntityPrefixIndex index: obj.getStructEntity().getPrefixIndexes()) {
            tryLoadField(columnFamily, obj.getId(), index.field, loadedValues);
            removeIndexedValue(index, obj.getId(), (String) loadedValues.get(index.field), modifiers);
        }

        // delete self-object
        modifiers.add(new ModifierRemove(columnFamily, FieldKey.buildKeyPrefix(obj.getId()), true));

        dataSource.modify(modifiers, transactionId);
    }

    @Override
    public <T, U extends DomainObject> T getValue(final EntityField field, U obj) throws DataSourceDatabaseException {
        ensureTransaction();

        byte[] value = dataSource.getValue(obj.getStructEntity().getColumnFamily(), new FieldKey(obj.getId(), field.getName()).pack(), transactionId);
        return (T) TypeConvert.unpack(field.getType(), value, field.getPacker());
    }

    @Override
    public long createIterator(String columnFamily, KeyPattern pattern) throws DataSourceDatabaseException {
        ensureTransaction();

        return dataSource.createIterator(columnFamily, pattern, transactionId);
    }

    public void commit() throws DataSourceDatabaseException {
        if (transactionId != -1) {
            dataSource.commitTransaction(transactionId);
            transactionId = -1;
        }
    }

    @Override
    public void close() throws DataSourceDatabaseException {
        if (transactionId != -1) {
            dataSource.rollbackTransaction(transactionId);
        }
    }

    private void ensureTransaction() throws DataSourceDatabaseException {
        if (transactionId == -1) {
            transactionId = dataSource.beginTransaction();
        }
    }

    private void tryLoadFields(String columnFamily, long id, final List<EntityField> fields, Map<EntityField, Object> loadedValues) throws DataSourceDatabaseException {
        for (EntityField field: fields) {
            tryLoadField(columnFamily, id, field, loadedValues);
        }
    }

    private void tryLoadField(String columnFamily, long id, EntityField field, Map<EntityField, Object> loadedValues) throws DataSourceDatabaseException {
        if (loadedValues.containsKey(field)) {
            return;
        }

        final byte[] key = new FieldKey(id, field.getName()).pack();
        final byte[] value = dataSource.getValue(columnFamily, key, transactionId);
        loadedValues.put(field, TypeConvert.unpack(field.getType(), value, field.getPacker()));
    }

    static void updateIndexedValue(EntityIndex index, long id, Map<EntityField, Object> prevValues, Map<EntityField, Object> newValues, List<Modifier> destination) {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        // Remove old value-index
        IndexUtils.setHashValues(index.sortedFields, prevValues, indexKey.getFieldValues());
        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));

        // Add new value-index
        for (int i = 0; i < index.sortedFields.size(); ++i) {
            EntityField field = index.sortedFields.get(i);
            Object value = newValues.containsKey(field) ? newValues.get(field) : prevValues.get(field);
            indexKey.getFieldValues()[i] = IndexUtils.buildHash(field.getType(), value);
        }
        destination.add(new ModifierSet(index.columnFamily, indexKey.pack()));
    }

    static void removeIndexedValue(EntityIndex index, long id, Map<EntityField, Object> values, List<Modifier> destination) {
        final IndexKey indexKey = new IndexKey(id, new long[index.sortedFields.size()]);

        IndexUtils.setHashValues(index.sortedFields, values, indexKey.getFieldValues());
        destination.add(new ModifierRemove(index.columnFamily, indexKey.pack(), false));
    }

    private void updateIndexedValue(EntityPrefixIndex index, long id, String prevTextValue, String newTextValue, List<Modifier> destination) throws DataSourceDatabaseException {
        List<String> deletingLexemes = new ArrayList<>();
        List<String> insertingLexemes = new ArrayList<>();
        PrefixIndexUtils.diffIndexedLexemes(prevTextValue, newTextValue, deletingLexemes, insertingLexemes);

        PrefixIndexUtils.removeIndexedLexemes(index, id, deletingLexemes, destination, dataSource, transactionId);
        PrefixIndexUtils.insertIndexedLexemes(index, id, insertingLexemes, destination, dataSource, transactionId);
    }

    private void removeIndexedValue(EntityPrefixIndex index, long id, String textValue, List<Modifier> destination) throws DataSourceDatabaseException {
        Collection<String> lexemes = PrefixIndexUtils.splitIndexingTextIntoLexemes(textValue);
        PrefixIndexUtils.removeIndexedLexemes(index, id, lexemes, destination, dataSource, transactionId);
    }

    private static boolean isChangedIndex(EntityIndex index, Map<EntityField, Object> newValues) {
        for (EntityField iField: index.sortedFields) {
            if (newValues.containsKey(iField)) {
                return true;
            }
        }
        return false;
    }

    private void validateUpdatingValue(DomainObject obj, EntityField field, Object value) throws DatabaseException {
        if (value == null) {
            return;
        }

        if (!foreignKeyEnabled || !field.isForeign()) {
            return;
        }

        long fkeyIdValue = ((Long) value).longValue();
        if (dataSource.getValue(obj.getStructEntity().getColumnFamily(), new FieldKey(fkeyIdValue).pack(), transactionId) == null) {
            throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), field, fkeyIdValue);
        }
    }

    private void validateRemovingObject(DomainObject obj) throws DatabaseException {
        if (!foreignKeyEnabled) {
            return;
        }

        List<StructEntity.Reference> references = obj.getStructEntity().getReferencingForeignFields();
        if (references.isEmpty()) {
            return;
        }

        KeyPattern keyPattern = IndexKey.buildKeyPattern(obj.getId());
        for (StructEntity.Reference ref : references) {
            long iteratorId = dataSource.createIterator(ref.fieldIndex.columnFamily, keyPattern, transactionId);
            try {
                KeyValue keyValue = dataSource.next(iteratorId);
                if (keyValue != null) {
                    long referencingId = IndexKey.unpackId(keyValue.getKey());
                    throw new ForeignDependencyException(obj.getId(), obj.getStructEntity().getObjectClass(), referencingId, ref.objClass);
                }
            } finally {
                dataSource.closeIterator(iteratorId);
            }
        }
    }
}
