package com.infomaximum.rocksdb.core.datasource;

import org.rocksdb.RocksDBException;

import java.util.Map;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    public long nextId(String columnFamily) throws RocksDBException;

    public Map<String, byte[]> load(String columnFamily, long id, boolean isReadOnly) throws RocksDBException;

    public void set(String columnFamily, long id, String field, byte[] value);
}
