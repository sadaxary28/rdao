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
import com.infomaximum.database.domainobject.key.KeyAvailability;
import com.infomaximum.database.domainobject.key.KeyField;
import com.infomaximum.database.domainobject.key.KeyIndex;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by user on 23.04.2017.
 */
public class Transaction {

    private final DataSource dataSource;
    private long transactionId = -1;

    public Transaction(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void update(StructEntity structEntity, DomainObject self, Map<Field, Object> loadValues, Map<Field, Object> writeValues) throws DataSourceDatabaseException {
        final String columnFamily = structEntity.annotationEntity.name();
        final List<Modifier> queue = new ArrayList<>();

        queue.add(new ModifierSet(columnFamily, new KeyAvailability(self.getId()).pack(), TypeConvert.pack(self.getId())));
        for (Map.Entry<Field, Object> writeEntry: writeValues.entrySet()) {
            Field field = writeEntry.getKey();
            Object value = writeEntry.getValue();

            String key = new KeyField(self.getId(), field.name()).pack();
            if (value!=null) {
                byte[] bValue = TypeConvert.packObject(value.getClass(), value);
                queue.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                queue.add(new ModifierRemove(columnFamily, key));
            }
        }

        //Разбираемся с индексами
        for (StructEntityIndex structEntityIndex: structEntity.getStructEntityIndices()){
            String indexColumnFamily = structEntityIndex.columnFamily;

            boolean isUpdateIndex = false;
            for(Field iField: structEntityIndex.indexFieldsSort) {
                if (writeValues.containsKey(iField)) {
                    isUpdateIndex=true;
                    break;
                }
            }
            if (!isUpdateIndex) continue;

            //Нужно обновлять индекс...
            int oldHash = 111;//TODO Необходимо вычислять
            queue.add(new ModifierRemove(indexColumnFamily, new KeyIndex(self.getId(), oldHash).pack()));


            //Вычисляем новый хеш
            List<Object> newValues = new ArrayList();
            for (Field field: structEntityIndex.indexFieldsSort) {
                newValues.add(writeValues.get(field));
            }

            int newHash = IndexUtils.calcHashValues(newValues);
            queue.add(new ModifierSet(indexColumnFamily, new KeyIndex(self.getId(), newHash).pack(), TypeConvert.pack(self.getId())));
        }

        modify(queue);
    }

    public void remove(StructEntity structEntity, DomainObject self) throws DataSourceDatabaseException {
        final String columnFamily = structEntity.annotationEntity.name();
        modify(Arrays.asList(ModifierRemove.removeDomainObject(columnFamily, self.getId())));
    }

    public void commit() throws DataSourceDatabaseException {
        if (transactionId != -1) {
            dataSource.commitTransaction(transactionId);
        }
    }

    private void modify(final List<Modifier> modifiers) throws DataSourceDatabaseException {
        if (transactionId == -1) {
            transactionId = dataSource.beginTransaction();
        }

        dataSource.modify(modifiers, transactionId);
    }
}
