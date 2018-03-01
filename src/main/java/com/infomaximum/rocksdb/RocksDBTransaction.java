package com.infomaximum.rocksdb;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.exception.DatabaseException;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

public class RocksDBTransaction implements DBTransaction {

    private final Transaction transaction;
    private final RocksDBProvider rocksDBProvider;

    RocksDBTransaction(Transaction transaction, RocksDBProvider rocksDBProvider) {
        this.transaction = transaction;
        this.rocksDBProvider = rocksDBProvider;
    }

    @Override
    public DBIterator createIterator(String columnFamily) throws DatabaseException {
        return buildIterator(rocksDBProvider.getColumnFamilyHandle(columnFamily));
    }

    @Override
    public long nextId(String sequenceName) throws DatabaseException {
        return rocksDBProvider.getSequenceManager().getSequence(sequenceName).next();
    }

    @Override
    public byte[] getValue(String columnFamily, final byte[] key) throws DatabaseException {
        try {
            return transaction.get(rocksDBProvider.getColumnFamilyHandle(columnFamily), rocksDBProvider.getReadOptions(), key);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void put(String columnFamily, byte[] key, byte[] value) throws DatabaseException {
        ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);
        try {
            transaction.put(columnFamilyHandle, key, value);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void delete(String columnFamily, byte[] key) throws DatabaseException {
        ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);
        try {
            transaction.delete(columnFamilyHandle, key);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void deleteRange(String columnFamily, byte[] keyPrefix) throws DatabaseException {
        final ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);

        try (RocksDBIterator i = buildIterator(columnFamilyHandle)) {
            for (KeyValue keyValue = i.seek(new KeyPattern(keyPrefix)); keyValue != null; keyValue = i.next()) {
                transaction.delete(columnFamilyHandle, keyValue.getKey());
            }
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void commit() throws DatabaseException {
        try {
            transaction.commit();
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void rollback() throws DatabaseException {
        try {
            transaction.rollback();
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() throws DatabaseException {
        transaction.close();
    }

    private RocksDBIterator buildIterator(ColumnFamilyHandle columnFamily) {
        return new RocksDBIterator(transaction.getIterator(rocksDBProvider.getReadOptions(), columnFamily));
    }
}
