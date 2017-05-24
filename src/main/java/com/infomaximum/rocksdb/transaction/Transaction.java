package com.infomaximum.rocksdb.transaction;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectFieldValueUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.index.IndexUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.key.Key;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyAvailability;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyField;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyIndex;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityIndex;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.struct.modifier.Modifier;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierRemove;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierSet;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.RocksDBException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by user on 23.04.2017.
 */
public class Transaction {

    private final DataSource dataSource;
    private List<Modifier> queue;
    private volatile AtomicBoolean active;

    public Transaction(DataSource dataSource) {
        this.dataSource = dataSource;
        this.queue = new ArrayList<Modifier>();
        this.active = new AtomicBoolean(true);
    }

    public boolean isActive() {
        return active.get();
    }

    public void update(StructEntity structEntity, DomainObject self, Set<Field> fields) throws IllegalAccessException {
        String columnFamily = structEntity.annotationEntity.columnFamily();

        queue.add(new ModifierSet(columnFamily, new KeyAvailability(self.getId()).pack(), TypeConvertRocksdb.pack(self.getId())));
        for (Field field: fields) {
            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
            String key = new KeyField(self.getId(), formatFieldName).pack();
            byte[] value = DomainObjectFieldValueUtils.packValue(self, field);
            if (value!=null) {
                queue.add(new ModifierSet(columnFamily, key, value));
            } else {
                queue.add(new ModifierRemove(columnFamily, key));
            }
        }

        //Разбираемся с индексами
        for (StructEntityIndex structEntityIndex: structEntity.indexs.values()){
            boolean isUpdateIndex = false;
            for(Field iField: structEntityIndex.indexFieldsSort) {
                if (fields.contains(iField)) {
                    isUpdateIndex=true;
                    break;
                }
            }
            if (!isUpdateIndex) continue;

            //Нужно обновлять индекс...
            int oldHash = 111;//TODO Необходимо вычислять
            queue.add(new ModifierRemove(columnFamily, new KeyIndex(self.getId(), structEntityIndex.name, oldHash).pack()));


            //Вычисляем новый хеш
            Object[] newValues = new Object[structEntityIndex.indexFieldsSort.size()];
            for (int i=0 ; i<newValues.length; i++) {
                Field iField = structEntityIndex.indexFieldsSort.get(i);
                newValues[i] = iField.get(self);
            }

            int newHash = IndexUtils.calcHashValues(newValues);
            queue.add(new ModifierSet(columnFamily, new KeyIndex(self.getId(), structEntityIndex.name, newHash).pack(), TypeConvertRocksdb.pack(self.getId())));
        }
    }

    public void remove(String columnFamily, DomainObject self) {
        queue.add(ModifierRemove.removeDomainObject(columnFamily, self.getId()));
    }

    public void commit() throws RocksDBException {
        //Ставим флаг, что транзакция больше не активна
        if (!active.compareAndSet(true, false)) throw new RuntimeException("Transaction is not active: is commited");

        //Комитим
        dataSource.commit(queue);

        //Чистим
        queue = null;
    }
}
