package com.infomaximum.rocksdb;

import com.google.common.primitives.UnsignedBytes;
import com.infomaximum.database.exception.SequenceNotFoundException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.exception.DatabaseException;
import org.rocksdb.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class RocksDBTransaction implements DBTransaction {

    private static final Comparator<byte[]> KEY_COMPARATOR = UnsignedBytes.lexicographicalComparator();

    private final Transaction transaction;
    private final RocksDBProvider rocksDBProvider;
    private final Map<String, RangeKey> singleDeletedKeys = new HashMap<>();

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
        SequenceManager.Sequence sequence = rocksDBProvider.getSequenceManager().getSequence(sequenceName);
        if (sequence == null) {
            throw new SequenceNotFoundException(sequenceName);
        }
        return sequence.next();
    }

    @Override
    public byte[] getValue(String columnFamily, byte[] key) throws DatabaseException {
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
    public void deleteRange(String columnFamily, byte[] beginKey, byte[] endKey) throws DatabaseException {
        deleteRange(columnFamily, beginKey, endKey, transaction::delete);
    }

    @Override
    public void singleDelete(String columnFamily, byte[] key) throws DatabaseException {
        ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);
        try {
            transaction.singleDelete(columnFamilyHandle, key);
            singleDeletedKeys.computeIfAbsent(columnFamily, s -> new RangeKey()).setKey(key);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void singleDeleteRange(String columnFamily, byte[] beginKey, byte[] endKey) throws DatabaseException {
        deleteRange(columnFamily, beginKey, endKey, transaction::singleDelete);

        RangeKey range = singleDeletedKeys.computeIfAbsent(columnFamily, s -> new RangeKey());
        range.setBegin(beginKey);
        range.setEnd(endKey);
    }

    private void deleteRange(String columnFamily, byte[] beginKey, byte[] endKey, BiConsumer<ColumnFamilyHandle, byte[]> deleteFunc) throws DatabaseException {
        ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(columnFamily);

        try (RocksIterator i = transaction.getIterator(rocksDBProvider.getReadOptions(), columnFamilyHandle)) {
            for (i.seek(beginKey); i.isValid(); i.next()) {
                byte[] key = i.key();
                if (key == null || KEY_COMPARATOR.compare(key, endKey) >= 0) {
                    break;
                }

                deleteFunc.accept(columnFamilyHandle, key);
            }

            i.status();
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void commit() throws DatabaseException {
        try {
            transaction.commit();

            for (Map.Entry<String, RangeKey> entry : singleDeletedKeys.entrySet()) {
                ColumnFamilyHandle columnFamilyHandle = rocksDBProvider.getColumnFamilyHandle(entry.getKey());
                rocksDBProvider.getRocksDB().compactRange(
                        columnFamilyHandle,
                        entry.getValue().begin,
                        entry.getValue().end,
                        true, -1, 0);
            }
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        } finally {
            singleDeletedKeys.clear();
        }
    }

    @Override
    public void rollback() throws DatabaseException {
        try {
            transaction.rollback();
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        } finally {
            singleDeletedKeys.clear();
        }
    }

    @Override
    public void close() throws DatabaseException {
        transaction.close();
    }

    private RocksDBIterator buildIterator(ColumnFamilyHandle columnFamily) {
        return new RocksDBIterator(transaction.getIterator(rocksDBProvider.getReadOptions(), columnFamily));
    }

    private static class RangeKey {

        byte[] begin = null;
        byte[] end = null;

        void setBegin(byte[] key) {
            if (begin == null || KEY_COMPARATOR.compare(key, begin) < 0) {
                begin = key;
            }
        }

        void setEnd(byte[] key) {
            if (end == null || KEY_COMPARATOR.compare(key, end) > 0) {
                end = key;
            }
        }

        void setKey(byte[] key) {
            if (begin == null) {
                begin = key;
                end = nextOf(Arrays.copyOf(key, key.length));
            } else {
                int res = KEY_COMPARATOR.compare(key, begin);
                if (res < 0) {
                    begin = key;
                } else if (res != 0) {
                    res = KEY_COMPARATOR.compare(key, end);
                    if (res > 0) {
                        end = nextOf(key);
                    }
                }
            }
        }

        private static byte[] nextOf(byte[] key) {
            int val = UnsignedBytes.toInt(key[key.length - 1]);
            if (val >= 0xff) {
                key = Arrays.copyOf(key, key.length + 1);
                val = 0;
            }
            key[key.length - 1] = UnsignedBytes.checkedCast(++val);
            return key;
        }
    }

    @FunctionalInterface
    private interface BiConsumer<T, U> {

        void accept(T t, U u) throws RocksDBException;
    }
}
