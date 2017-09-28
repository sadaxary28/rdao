package com.infomaximum.database.core.transaction;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.index.IndexUtils;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.core.transaction.modifier.Modifier;
import com.infomaximum.database.core.transaction.modifier.ModifierRemove;
import com.infomaximum.database.core.transaction.modifier.ModifierSet;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.*;

/**
 * Created by user on 23.04.2017.
 */
public class Transaction implements AutoCloseable {

    private final DataSource dataSource;
    private long transactionId = -1;

    public Transaction(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void update(StructEntity structEntity, DomainObject self, Map<Field, Object> loadedValues, Map<Field, Object> newValues) throws DataSourceDatabaseException {
        ensureTransaction();

        final String columnFamily = structEntity.annotationEntity.name();
        final List<Modifier> modifiers = new ArrayList<>();

        // update self-object
        modifiers.add(new ModifierSet(columnFamily, new FieldKey(self.getId()).pack()));
        for (Map.Entry<Field, Object> writeEntry: newValues.entrySet()) {
            Field field = writeEntry.getKey();
            Object value = writeEntry.getValue();

            byte[] key = new FieldKey(self.getId(), field.name()).pack();
            if (value != null) {
                byte[] bValue = TypeConvert.packObject(value.getClass(), value);
                modifiers.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                modifiers.add(new ModifierRemove(columnFamily, key, false));
            }
        }

        // update indexed values
        for (StructEntityIndex structEntityIndex: structEntity.getStructEntityIndices()){
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

            tryLoadFields(columnFamily, self.getId(), structEntityIndex.sortedFields, loadedValues);

            final IndexKey indexKey = new IndexKey(self.getId(), new long[structEntityIndex.sortedFields.size()]);

            // Remove old value-index
            setHashValues(structEntityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
            modifiers.add(new ModifierRemove(indexColumnFamily, indexKey.pack(), false));

            // Add new value-index
            setHashValues(structEntityIndex.sortedFields, newValues, indexKey.getFieldValues());
            modifiers.add(new ModifierSet(indexColumnFamily, indexKey.pack()));
        }

        dataSource.modify(modifiers, transactionId);
    }

    public void remove(StructEntity structEntity, DomainObject self) throws DataSourceDatabaseException {
        ensureTransaction();

        final String columnFamily = structEntity.annotationEntity.name();
        final List<Modifier> modifiers = new ArrayList<>();

        // delete self-object
        modifiers.add(new ModifierRemove(columnFamily, FieldKey.buildKeyPrefix(self.getId()), true));

        // delete indexed values
        if (!structEntity.getStructEntityIndices().isEmpty()) {
            Map<Field, Object> loadedValues = new HashMap<>();

            for (StructEntityIndex structEntityIndex : structEntity.getStructEntityIndices()) {
                tryLoadFields(columnFamily, self.getId(), structEntityIndex.sortedFields, loadedValues);

                final IndexKey indexKey = new IndexKey(self.getId(), new long[structEntityIndex.sortedFields.size()]);

                setHashValues(structEntityIndex.sortedFields, loadedValues, indexKey.getFieldValues());
                modifiers.add(new ModifierRemove(structEntityIndex.columnFamily, indexKey.pack(), false));
            }
        }

        dataSource.modify(modifiers, transactionId);
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

    private void tryLoadFields(final String columnFamily, final long id, final List<Field> fields, Map<Field, Object> loadedValues) throws DataSourceDatabaseException {
        for (Field field: fields) {
            if (loadedValues.containsKey(field)) {
                continue;
            }

            final byte[] key = new FieldKey(id, field.name()).pack();
            final byte[] value = dataSource.getValue(columnFamily, key, transactionId);
            loadedValues.put(field, TypeConvert.get(field.type(), value));
        }
    }

    private static void setHashValues(final List<Field> sortedFields, final Map<Field, Object> values, long[] destination) {
        for (int i = 0; i < sortedFields.size(); ++i) {
            Field field = sortedFields.get(i);
            destination[i] = IndexUtils.buildHash(values.get(field), field.type());
        }
    }
}
