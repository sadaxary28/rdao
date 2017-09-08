package com.infomaximum.database.core.transaction;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.index.IndexUtils;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.core.transaction.struct.modifier.Modifier;
import com.infomaximum.database.core.transaction.struct.modifier.ModifierRemove;
import com.infomaximum.database.core.transaction.struct.modifier.ModifierSet;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.key.KeyAvailability;
import com.infomaximum.database.domainobject.key.KeyField;
import com.infomaximum.database.domainobject.key.KeyIndex;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    //    public void update(StructEntity structEntity, DomainObject self, Set<Field> fields, ConcurrentMap<String, Optional<Object>> loadFieldValues, ConcurrentMap<String, Optional<Object>> writeFieldValues) throws IllegalAccessException {
    public void update(StructEntity structEntity, DomainObject self, Map<Field, Object> loadValues, Map<Field, Object> writeValues) {
        String columnFamily = structEntity.annotationEntity.name();

        queue.add(new ModifierSet(columnFamily, new KeyAvailability(self.getId()).pack(), TypeConvertRocksdb.pack(self.getId())));
        for (Map.Entry<Field, Object> writeEntry: writeValues.entrySet()) {
            Field field = writeEntry.getKey();
            Object value = writeEntry.getValue();

            String key = new KeyField(self.getId(), field.name()).pack();
            if (value!=null) {
                byte[] bValue = TypeConvertRocksdb.packObject(value.getClass(), value);
                queue.add(new ModifierSet(columnFamily, key, bValue));
            } else {
                queue.add(new ModifierRemove(columnFamily, key));
            }
        }

        //Разбираемся с индексами
        for (StructEntityIndex structEntityIndex: structEntity.indexs.values()){
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
            queue.add(new ModifierRemove(columnFamily, new KeyIndex(self.getId(), structEntityIndex.name, oldHash).pack()));


            //Вычисляем новый хеш
            List<Object> newValues = new ArrayList();
            for (Field field: structEntityIndex.indexFieldsSort) {
                newValues.add(writeValues.get(field));
            }

            int newHash = IndexUtils.calcHashValues(newValues);
            queue.add(new ModifierSet(columnFamily, new KeyIndex(self.getId(), structEntityIndex.name, newHash).pack(), TypeConvertRocksdb.pack(self.getId())));
        }
    }

    public void remove(StructEntity structEntity, DomainObject self) {
        String columnFamily = structEntity.annotationEntity.name();
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
