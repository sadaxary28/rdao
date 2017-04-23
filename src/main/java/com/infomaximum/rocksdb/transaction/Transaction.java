package com.infomaximum.rocksdb.transaction;

import org.rocksdb.RocksDBException;

/**
 * Created by user on 23.04.2017.
 */
public interface Transaction {

    void put(String columnFamily, String key, byte[] value);

    void commit() throws RocksDBException;
}
