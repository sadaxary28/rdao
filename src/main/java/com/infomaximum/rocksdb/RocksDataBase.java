package com.infomaximum.rocksdb;

import com.infomaximum.database.exception.runtime.ColumnFamilyNotFoundException;
import com.infomaximum.database.utils.TypeConvert;
import org.rocksdb.*;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by kris on 10.03.17.
 */
public class RocksDataBase implements AutoCloseable {

    public static final String DEFAULT_COLUMN_FAMILY = "default";

    private final OptimisticTransactionDB rocksDB;
    private final ConcurrentMap<String, ColumnFamilyHandle> columnFamilies;
    private final WriteOptions writeOptions = new WriteOptions();
    private final ReadOptions readOptions = new ReadOptions();

    protected RocksDataBase(OptimisticTransactionDB rocksDB, ConcurrentMap<String, ColumnFamilyHandle> columnFamilies) throws RocksDBException {
        this.rocksDB = rocksDB;
        this.columnFamilies = columnFamilies;
    }

    public RocksDB getRocksDB() {
        return rocksDB.getBaseDB();
    }

    public ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) {
        ColumnFamilyHandle cf = columnFamilies.get(columnFamilyName);
        if (cf == null) {
            throw new ColumnFamilyNotFoundException(columnFamilyName);
        }
        return cf;
    }

    public ColumnFamilyHandle getDefaultColumnFamily() {
        return columnFamilies.get(RocksDataBase.DEFAULT_COLUMN_FAMILY);
    }

    public Map<String, ColumnFamilyHandle> getColumnFamilies() {
        return columnFamilies;
    }

    public Transaction beginTransaction() {
        return rocksDB.beginTransaction(writeOptions);
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public ReadOptions getReadOptions() {
        return readOptions;
    }

    public ColumnFamilyHandle createColumnFamily(String columnFamilyName) throws RocksDBException {
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(TypeConvert.pack(columnFamilyName));
        ColumnFamilyHandle columnFamilyHandle = getRocksDB().createColumnFamily(columnFamilyDescriptor);
        columnFamilies.put(columnFamilyName, columnFamilyHandle);
        return columnFamilyHandle;
    }

    public void dropColumnFamily(String columnFamilyName) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = columnFamilies.remove(columnFamilyName);
        if (columnFamilyHandle != null) {
            getRocksDB().dropColumnFamily(columnFamilyHandle);
            columnFamilyHandle.close();
        }
    }

    @Override
    public void close() {
        readOptions.close();
        writeOptions.close();

        for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilies.entrySet()) {
            entry.getValue().close();
        }
        columnFamilies.clear();

        rocksDB.close();
    }
}
