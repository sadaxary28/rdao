package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.iterator.IteratorEntityImpl;
import com.infomaximum.database.core.iterator.IteratorFindEntityImpl;
import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.domainobject.utils.DomainIndexUtils;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.*;

public class Transaction implements AutoCloseable, DataEnumerable {

    private final DataSource dataSource;
    private long transactionId = -1;

    protected Transaction(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        try {
            StructEntity entity = StructEntity.getInstance(clazz);

            long id = dataSource.nextId(entity.getName());

            T domainObject = DomainObjectUtils.buildDomainObject(clazz, id, this);

            //TODO нужно сделать "похорошему", без такого "хака"
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

        Map<EntityField, Object> loadedValues = object.getLoadedValues();
        Map<EntityField, Object> newValues = object.getNewValues();

        final String columnFamily = object.getStructEntity().getName();
        final List<Modifier> modifiers = new ArrayList<>();

        // update self-object
        modifiers.add(new ModifierSet(columnFamily, new FieldKey(object.getId()).pack()));
        for (Map.Entry<EntityField, Object> writeEntry: newValues.entrySet()) {
            EntityField field = writeEntry.getKey();
            Object value = writeEntry.getValue();

            byte[] key = new FieldKey(object.getId(), field.getName()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value, field.getPacker());
                modifiers.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                modifiers.add(new ModifierRemove(columnFamily, key, false));
            }
        }

        // update indexed values
        for (EntityIndex entityIndex : object.getStructEntity().getIndices()){
            String indexColumnFamily = entityIndex.columnFamily;

            boolean isUpdateIndex = false;
            for (EntityField iField: entityIndex.sortedFields) {
                if (newValues.containsKey(iField)) {
                    isUpdateIndex = true;
                    break;
                }
            }
            if (!isUpdateIndex) {
                continue;
            }

            tryLoadFields(columnFamily, object.getId(), entityIndex.sortedFields, loadedValues);

            final IndexKey indexKey = new IndexKey(object.getId(), new long[entityIndex.sortedFields.size()]);

            // Remove old value-index
            DomainIndexUtils.setHashValues(entityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
            modifiers.add(new ModifierRemove(indexColumnFamily, indexKey.pack(), false));

            // Add new value-index
            DomainIndexUtils.setHashValues(entityIndex.sortedFields, newValues, indexKey.getFieldValues());
            modifiers.add(new ModifierSet(indexColumnFamily, indexKey.pack()));
        }

        dataSource.modify(modifiers, transactionId);

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T object) throws DatabaseException {
        ensureTransaction();

        final String columnFamily = object.getStructEntity().getName();
        final List<Modifier> modifiers = new ArrayList<>();

        // delete self-object
        modifiers.add(new ModifierRemove(columnFamily, FieldKey.buildKeyPrefix(object.getId()), true));

        // delete indexed values
        if (!object.getStructEntity().getIndices().isEmpty()) {
            Map<EntityField, Object> loadedValues = new HashMap<>();

            for (EntityIndex entityIndex : object.getStructEntity().getIndices()) {
                tryLoadFields(columnFamily, object.getId(), entityIndex.sortedFields, loadedValues);

                final IndexKey indexKey = new IndexKey(object.getId(), new long[entityIndex.sortedFields.size()]);

                DomainIndexUtils.setHashValues(entityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
                modifiers.add(new ModifierRemove(entityIndex.columnFamily, indexKey.pack(), false));
            }
        }

        dataSource.modify(modifiers, transactionId);
    }

    @Override
    public <T extends Object, U extends DomainObject> T getValue(final EntityField field, U object) throws DataSourceDatabaseException {
        byte[] value = dataSource.getValue(object.getStructEntity().getName(), new FieldKey(object.getId(), field.getName()).pack(), transactionId);
        return (T) TypeConvert.unpack(field.getType(), value, field.getPacker());
    }

    @Override
    public <T extends DomainObject> T get(Class<T> clazz, Set<String> loadingFields, long id) throws DataSourceDatabaseException {
        ensureTransaction();

        String columnFamily = StructEntity.getInstance(clazz).getName();
        KeyPattern pattern = FieldKey.buildKeyPattern(id, loadingFields != null ? loadingFields : Collections.emptySet());

        long iteratorId = dataSource.createIterator(columnFamily, pattern, transactionId);

        T obj;
        try {
            obj = DomainObjectUtils.nextObject(clazz, dataSource, iteratorId, this, null);
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        return obj;
    }

    @Override
    public <T extends DomainObject> IteratorEntity<T> iterator(Class<T> clazz, Set<String> loadingFields) throws DatabaseException {
        ensureTransaction();
        return new IteratorEntityImpl(dataSource, this, clazz, loadingFields, transactionId);
    }

    @Override
    public <T extends DomainObject> IteratorEntity<T> find(Class<T> clazz, Set<String> loadingFields, Map<String, Object> filters) throws DatabaseException {
        ensureTransaction();
        return new IteratorFindEntityImpl(dataSource, this, clazz, loadingFields, filters, transactionId);
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
            if (loadedValues.containsKey(field)) {
                continue;
            }

            final byte[] key = new FieldKey(id, field.getName()).pack();
            final byte[] value = dataSource.getValue(columnFamily, key, transactionId);
            loadedValues.put(field, TypeConvert.unpack(field.getType(), value, field.getPacker()));
        }
    }
}
