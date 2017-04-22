package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.struct.RocksDataBase;

import java.util.Map;

/**
 * Created by user on 20.04.2017.
 */
public class DataSourceImpl implements DataSource {

    private final RocksDataBase rocksDataBase;

    public DataSourceImpl(RocksDataBase rocksDataBase) {
        this.rocksDataBase = rocksDataBase;
    }

    @Override
    public long nextId(String columnFamily) {
        return 0;
    }

    @Override
    public Map<String, byte[]> load(String columnFamily, long id, boolean isReadOnly) {
        return null;
    }

    @Override
    public void set(String columnFamily, long id, String field, byte[] value) {

    }
}
