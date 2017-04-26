package com.infomaximum.rocksdb.transaction.engine.impl;

import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by user on 23.04.2017.
 */
public class TransactionImpl implements Transaction {

    private final RocksDataBase rocksDataBase;
    private final Map<String, Map<String, byte[]>> queue;

    private boolean active;

    public TransactionImpl(RocksDataBase rocksDataBase) {
        this.rocksDataBase = rocksDataBase;
        this.queue = new HashMap<String, Map<String, byte[]>>();

        this.active=true;
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void put(String columnFamily, String key, byte[] value) {
        Map<String, byte[]> columnFamilyItem = queue.get(columnFamily);
        if (columnFamilyItem==null) {
            synchronized (queue) {
                columnFamilyItem = queue.get(columnFamily);
                if (columnFamilyItem==null) {
                    columnFamilyItem = new ConcurrentHashMap<String, byte[]>();
                    queue.put(columnFamily, columnFamilyItem);
                }
            }
        }
        columnFamilyItem.put(key, value);
    }



    @Override
    public void commit() throws RocksDBException {
        if (!active) throw new RuntimeException("Transaction is not active: is commited");

        //Комитим
        for (Map.Entry<String, Map<String, byte[]>> entryFamilyName: queue.entrySet()) {
            String columnFamilyName = entryFamilyName.getKey();
            Map<String, byte[]> values = entryFamilyName.getValue();

            for (Map.Entry<String, byte[]> entry: values.entrySet()) {
                ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamilyName);
                String key = entry.getKey();
                byte[] value = entry.getValue();
                rocksDataBase.getRocksDB().put(columnFamilyHandle, TypeConvertRocksdb.pack(key), value);
            }
        }

        //все ставим флаг, что транзакция больше не активна
        active=false;
    }
}
