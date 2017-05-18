package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.struct.modifier.Modifier;
import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public interface DataSource {

    public long nextId(String columnFamily) throws RocksDBException;

    public byte[] getField(String columnFamily, long id, String field) throws RocksDBException;

    public EntitySource getObject(String columnFamily, long id, Set<String> fields) throws RocksDBException;

    public EntitySource lockObject(String columnFamily, long id, Set<String> fields) throws RocksDBException;

    public EntitySource next(String columnFamily, Long prevId, Set<String> fields) throws RocksDBException;

    public void commit(List<Modifier> modifiers) throws RocksDBException;
}
