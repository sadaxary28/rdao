package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.iterator.IteratorEntityImpl;
import com.infomaximum.database.core.iterator.IteratorFindEntityImpl;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.utils.IndexUtils;
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
            Entity entityAnnotation = StructEntity.getInstance(clazz).annotationEntity;

            long id = dataSource.nextId(entityAnnotation.name());

            T domainObject = DomainObjectUtils.buildDomainObject(clazz, id, this);

            //TODO нужно сделать "похорошему", без такого "хака"
            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (Field field: entityAnnotation.fields()) {
                domainObject.set(field.name(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T object) throws DatabaseException {
        ensureTransaction();

        Map<Field, Object> loadedValues = object.getLoadedValues();
        Map<Field, Object> newValues = object.getNewValues();

        final String columnFamily = object.getStructEntity().annotationEntity.name();
        final List<Modifier> modifiers = new ArrayList<>();

        // update self-object
        modifiers.add(new ModifierSet(columnFamily, new FieldKey(object.getId()).pack()));
        for (Map.Entry<Field, Object> writeEntry: newValues.entrySet()) {
            Field field = writeEntry.getKey();
            Object value = writeEntry.getValue();

            byte[] key = new FieldKey(object.getId(), field.name()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.pack(value.getClass(), value);
                modifiers.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                modifiers.add(new ModifierRemove(columnFamily, key, false));
            }
        }

        // update indexed values
        for (StructEntityIndex structEntityIndex: object.getStructEntity().getStructEntityIndices()){
            String indexColumnFamily = structEntityIndex.columnFamily;

            boolean isUpdateIndex = false;
            for (Field iField: structEntityIndex.sortedFields) {
                if (newValues.containsKey(iField)) {
                    isUpdateIndex = true;
                    break;
                }
            }
            if (!isUpdateIndex) {
                continue;
            }

            tryLoadFields(columnFamily, object.getId(), structEntityIndex.sortedFields, loadedValues);

            final IndexKey indexKey = new IndexKey(object.getId(), new long[structEntityIndex.sortedFields.size()]);

            // Remove old value-index
            setHashValues(structEntityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
            modifiers.add(new ModifierRemove(indexColumnFamily, indexKey.pack(), false));

            // Add new value-index
            setHashValues(structEntityIndex.sortedFields, newValues, indexKey.getFieldValues());
            modifiers.add(new ModifierSet(indexColumnFamily, indexKey.pack()));
        }

        dataSource.modify(modifiers, transactionId);

        object._flushNewValues();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T object) throws DatabaseException {
        ensureTransaction();

        final String columnFamily = object.getStructEntity().annotationEntity.name();
        final List<Modifier> modifiers = new ArrayList<>();

        // delete self-object
        modifiers.add(new ModifierRemove(columnFamily, FieldKey.buildKeyPrefix(object.getId()), true));

        // delete indexed values
        if (!object.getStructEntity().getStructEntityIndices().isEmpty()) {
            Map<Field, Object> loadedValues = new HashMap<>();

            for (StructEntityIndex structEntityIndex : object.getStructEntity().getStructEntityIndices()) {
                tryLoadFields(columnFamily, object.getId(), structEntityIndex.sortedFields, loadedValues);

                final IndexKey indexKey = new IndexKey(object.getId(), new long[structEntityIndex.sortedFields.size()]);

                setHashValues(structEntityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
                modifiers.add(new ModifierRemove(structEntityIndex.columnFamily, indexKey.pack(), false));
            }
        }

        dataSource.modify(modifiers, transactionId);
    }

    @Override
    public <T extends Object, U extends DomainObject> T getField(final Class<T> type, String fieldName, U object) throws DataSourceDatabaseException {
        byte[] value = dataSource.getValue(object.getStructEntity().annotationEntity.name(), new FieldKey(object.getId(), fieldName).pack(), transactionId);
        return (T) TypeConvert.unpack(type, value);
    }

    @Override
    public <T extends DomainObject> T get(Class<T> clazz, Set<String> loadingFields, long id) throws DataSourceDatabaseException {
        ensureTransaction();

        String columnFamily = StructEntity.getInstance(clazz).annotationEntity.name();
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

    private void tryLoadFields(String columnFamily, long id, final List<Field> fields, Map<Field, Object> loadedValues) throws DataSourceDatabaseException {
        for (Field field: fields) {
            if (loadedValues.containsKey(field)) {
                continue;
            }

            final byte[] key = new FieldKey(id, field.name()).pack();
            final byte[] value = dataSource.getValue(columnFamily, key, transactionId);
            loadedValues.put(field, TypeConvert.unpack(field.type(), value));
        }
    }

    private static void setHashValues(final List<Field> sortedFields, final Map<Field, Object> values, long[] destination) {
        for (int i = 0; i < sortedFields.size(); ++i) {
            Field field = sortedFields.get(i);
            destination[i] = IndexUtils.buildHash(field.type(), values.get(field));
        }
    }
}
