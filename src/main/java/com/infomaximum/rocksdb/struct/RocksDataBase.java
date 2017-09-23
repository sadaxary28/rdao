package com.infomaximum.rocksdb.struct;

import com.infomaximum.database.utils.TypeConvert;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by kris on 10.03.17.
 */
public class RocksDataBase implements AutoCloseable {

    public static final String DEFAULT_COLUMN_FAMILY = "default";

    private final RocksDB rocksDB;
    private final ConcurrentMap<String, ColumnFamilyHandle> columnFamilies;

    public RocksDataBase(RocksDB rocksDB, ConcurrentMap<String, ColumnFamilyHandle> columnFamilies) throws RocksDBException {
        this.rocksDB = rocksDB;
        this.columnFamilies = columnFamilies;
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    public ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) {
        return columnFamilies.get(columnFamilyName);
    }

    public ColumnFamilyHandle getDefaultColumnFamily() {
        return columnFamilies.get(RocksDataBase.DEFAULT_COLUMN_FAMILY);
    }

    public Map<String, ColumnFamilyHandle> getColumnFamilies() {
        return columnFamilies;
    }

    public ColumnFamilyHandle createColumnFamily(String columnFamilyName) throws RocksDBException {
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(TypeConvert.pack(columnFamilyName));
        ColumnFamilyHandle columnFamilyHandle = rocksDB.createColumnFamily(columnFamilyDescriptor);
        columnFamilies.put(columnFamilyName, columnFamilyHandle);
        return columnFamilyHandle;
    }

    public void dropColumnFamily(String columnFamilyName) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = columnFamilies.remove(columnFamilyName);
        rocksDB.dropColumnFamily(columnFamilyHandle);
        columnFamilyHandle.close();
    }

    @Override
    public void close() {
        for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilies.entrySet()) {
            entry.getValue().close();
        }
        columnFamilies.clear();

        rocksDB.close();
    }
}
