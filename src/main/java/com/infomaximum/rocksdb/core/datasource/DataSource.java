package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    public Transaction createTransaction();

    public long nextId(String columnFamily) throws RocksDBException;

    public byte[] get(String columnFamily, long id, String field) throws RocksDBException;

    public Map<String, byte[]> gets(String columnFamily, long id, Set<String> fields) throws RocksDBException;

    public Map<String, byte[]> lock(String columnFamily, long id, Set<String> fields) throws RocksDBException;

}
