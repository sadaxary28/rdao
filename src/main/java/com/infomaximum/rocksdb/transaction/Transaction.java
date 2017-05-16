package com.infomaximum.rocksdb.transaction;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectFieldValueUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyAvailability;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyField;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by user on 23.04.2017.
 */
public class Transaction {

    private final RocksDataBase rocksDataBase;
    private final Map<String, Map<String, Optional<byte[]>>> queue;

    private boolean active;

    public Transaction(RocksDataBase rocksDataBase) {
        this.rocksDataBase = rocksDataBase;
        this.queue = new HashMap<String, Map<String, Optional<byte[]>>>();

        this.active=true;
    }

    public boolean isActive() {
        return active;
    }

    public void update(String columnFamily, DomainObject self, Set<Field> fields) throws IllegalAccessException {
        pull(columnFamily, new KeyAvailability(self.getId()).pack(), TypeConvertRocksdb.pack(self.getId()));
        for (Field field: fields) {
            String formatFieldName = StructEntityUtils.getFormatFieldName(field);
            String key = new KeyField(self.getId(), formatFieldName).pack();
            byte[] value = DomainObjectFieldValueUtils.packValue(self, field);
            pull(columnFamily, key, value);
        }
    }

    private void pull(String columnFamily, String key, byte[] value) {
        Map<String, Optional<byte[]>> columnFamilyItem = queue.get(columnFamily);
        if (columnFamilyItem==null) {
            synchronized (queue) {
                columnFamilyItem = queue.get(columnFamily);
                if (columnFamilyItem==null) {
                    columnFamilyItem = new ConcurrentHashMap<String, Optional<byte[]>>();
                    queue.put(columnFamily, columnFamilyItem);
                }
            }
        }
        columnFamilyItem.put(key, Optional.ofNullable(value));
    }

    public void commit() throws RocksDBException {
        if (!active) throw new RuntimeException("Transaction is not active: is commited");

        //Комитим
        for (Map.Entry<String, Map<String, Optional<byte[]>>> entryFamilyName: queue.entrySet()) {
            String columnFamilyName = entryFamilyName.getKey();
            Map<String, Optional<byte[]>> values = entryFamilyName.getValue();

            for (Map.Entry<String, Optional<byte[]>> entry: values.entrySet()) {
                ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamilyName);
                String key = entry.getKey();
                if (entry.getValue().isPresent()) {
                    rocksDataBase.getRocksDB().put(columnFamilyHandle, TypeConvertRocksdb.pack(key), entry.getValue().get());
                } else {
                    rocksDataBase.getRocksDB().delete(columnFamilyHandle, TypeConvertRocksdb.pack(key));
                }
            }
        }

        //все ставим флаг, что транзакция больше не активна
        active=false;
    }
}
