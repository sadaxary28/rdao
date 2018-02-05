package com.infomaximum.rocksdb;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.infomaximum.database.core.sequence.SequenceManager;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierRemove;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.exception.DataSourceDatabaseException;
import com.infomaximum.database.exception.IteratorNotFoundException;
import com.infomaximum.database.exception.TransactionNotFoundException;
import com.infomaximum.database.utils.ByteUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by user on 20.04.2017.
 */
public class RocksDBDataSource implements DataSource {

    private final long ROCKS_OBJECT_LIFE_TIME_IN_MIN = 10;

    private final RocksDataBase rocksDataBase;
    private final SequenceManager sequenceManager;

    private final AtomicLong seqRocksObject;
    private final Cache<Object, Object> iterators;
    private final Cache<Object, Object> transactions;

    private static class IteratorWrap {

        public final RocksIterator iterator;
        private KeyPattern pattern;

        IteratorWrap(RocksIterator iterator) {
            this.iterator = iterator;
        }

        void seek(KeyPattern pattern) {
            this.pattern = pattern;

            if (pattern == null || pattern.getPrefix() == null) {
                iterator.seekToFirst();
            }
            else {
                iterator.seek(pattern.getPrefix());
            }
        }

        KeyValue getKeyValue() throws DataSourceDatabaseException {
            if (iterator.isValid()) {
                return new KeyValue(iterator.key(), iterator.value());
            }

            throwIfFail();
            return null;
        }

        KeyValue findMatched() throws DataSourceDatabaseException {
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (pattern != null) {
                    int matchResult = pattern.match(key);
                    if (matchResult == KeyPattern.MATCH_RESULT_CONTINUE) {
                        iterator.next();
                        continue;
                    } else if (matchResult == KeyPattern.MATCH_RESULT_UNSUCCESS) {
                        return null;
                    }
                }

                return new KeyValue(key, iterator.value());
            }

            throwIfFail();
            return null;
        }

        private void throwIfFail() throws DataSourceDatabaseException {
            try {
                iterator.status();
            } catch (RocksDBException e) {
                throw new DataSourceDatabaseException(e);
            }
        }
    }

    public RocksDBDataSource(RocksDataBase rocksDataBase) throws RocksDBException {
        this.rocksDataBase = rocksDataBase;
        this.sequenceManager = new SequenceManager(rocksDataBase);

        this.seqRocksObject = new AtomicLong(1);
        this.iterators = CacheBuilder.newBuilder()
                .expireAfterAccess(ROCKS_OBJECT_LIFE_TIME_IN_MIN, TimeUnit.MINUTES)
                .removalListener(notification -> {
                    IteratorWrap iterator = (IteratorWrap) notification.getValue();
                    iterator.iterator.close();
                })
                .build();
        this.transactions = CacheBuilder.newBuilder()
                .expireAfterAccess(ROCKS_OBJECT_LIFE_TIME_IN_MIN, TimeUnit.MINUTES)
                .removalListener(notification -> {
                    Transaction transaction = (Transaction) notification.getValue();
                    transaction.close();
                })
                .build();
    }

    @Override
    public long nextId(String entityName) throws DataSourceDatabaseException {
        try {
            return sequenceManager.getSequence(entityName).next();
        } catch (Exception e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public byte[] getValue(String columnFamily, final byte[] key) throws DataSourceDatabaseException {
        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
            return rocksDataBase.getRocksDB().get(columnFamilyHandle, rocksDataBase.getReadOptions(), key);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public byte[] getValue(String columnFamily, final byte[] key, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
            return transaction.get(columnFamilyHandle, rocksDataBase.getReadOptions(), key);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public void modify(final List<Modifier> modifiers, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        try {
            for (Modifier modifier : modifiers) {
                ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(modifier.columnFamily);

                if (modifier instanceof ModifierSet) {
                    ModifierSet modifierSet = (ModifierSet) modifier;
                    transaction.put(columnFamilyHandle, modifier.getKey(), modifierSet.getValue());
                } else if (modifier instanceof ModifierRemove) {
                    ModifierRemove modifierRemove = (ModifierRemove)modifier;
                    if (modifierRemove.isKeyPrefix()) {
                        try (RocksIterator iterator = transaction.getIterator(rocksDataBase.getReadOptions(), columnFamilyHandle)) {
                            iterator.seek(modifierRemove.getKey());
                            while (true) {
                                if (!iterator.isValid()){
                                    iterator.status();
                                    break;
                                }
                                final byte[] foundKey = iterator.key();
                                if (!ByteUtils.startsWith(modifierRemove.getKey(), foundKey)) {
                                    break;
                                }
                                transaction.delete(columnFamilyHandle, foundKey);
                                iterator.next();
                            }
                        }
                    } else {
                        transaction.delete(columnFamilyHandle, modifier.getKey());
                    }
                } else {
                    throw new RuntimeException("Not support type modifier: " + modifier.getClass());
                }
            }
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public long beginTransaction() throws DataSourceDatabaseException {
        long transactionId = seqRocksObject.getAndIncrement();
        transactions.put(transactionId, rocksDataBase.beginTransaction());
        return transactionId;
    }

    @Override
    public void commitTransaction(long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        try {
            transaction.commit();
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        } finally {
            transactions.invalidate(transactionId);
        }
    }

    @Override
    public void rollbackTransaction(long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            return;
        }

        try {
            transaction.rollback();
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        } finally {
            transactions.invalidate(transactionId);
        }
    }

    @Override
    public long createIterator(String columnFamily) throws DataSourceDatabaseException {
        return createIterator(columnFamily, columnFamilyHandle -> rocksDataBase.getRocksDB().newIterator(columnFamilyHandle, rocksDataBase.getReadOptions()));
    }

    @Override
    public long createIterator(String columnFamily, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        return createIterator(columnFamily, columnFamilyHandle -> transaction.getIterator(rocksDataBase.getReadOptions(), columnFamilyHandle));
    }

    private long createIterator(final String columnFamily, Function<ColumnFamilyHandle, RocksIterator> iteratorGetter) {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        RocksIterator rocksIterator = iteratorGetter.apply(columnFamilyHandle);

        long iteratorId = seqRocksObject.getAndIncrement();
        iterators.put(iteratorId, new IteratorWrap(rocksIterator));

        return iteratorId;
    }

    @Override
    public KeyValue seek(long iteratorId, final KeyPattern pattern) throws DataSourceDatabaseException {
        IteratorWrap iter = (IteratorWrap) iterators.getIfPresent(iteratorId);
        if (iter == null) {
            throw new IteratorNotFoundException(iteratorId);
        }

        iter.seek(pattern);
        return iter.findMatched();
    }

    @Override
    public KeyValue next(long iteratorId) throws DataSourceDatabaseException {
        IteratorWrap iter = (IteratorWrap) iterators.getIfPresent(iteratorId);
        if (iter == null) {
            throw new IteratorNotFoundException(iteratorId);
        }

        iter.iterator.next();
        return iter.findMatched();
    }

    @Override
    public KeyValue step(long iteratorId, StepDirection direction) throws DataSourceDatabaseException {
        IteratorWrap iter = (IteratorWrap) iterators.getIfPresent(iteratorId);
        if (iter == null) {
            throw new IteratorNotFoundException(iteratorId);
        }

        switch (direction) {
            case FORWARD:
                iter.iterator.next();
                break;
            case BACKWARD:
                iter.iterator.prev();
                break;
        }

        return iter.getKeyValue();
    }

    @Override
    public void closeIterator(long iteratorId) {
        iterators.invalidate(iteratorId);
    }

    @Override
    public boolean containsColumnFamily(String name) {
        return rocksDataBase.getColumnFamilies().containsKey(name);
    }

    @Override
    public String[] getColumnFamilies() {
        int size = rocksDataBase.getColumnFamilies().size();
        if (rocksDataBase.getColumnFamilies().containsKey(RocksDataBase.DEFAULT_COLUMN_FAMILY)) {
            --size;
        }

        String[] columnFamilies = new String[size];
        int pos = 0;
        for (Map.Entry<String, ColumnFamilyHandle> cf : rocksDataBase.getColumnFamilies().entrySet()) {
            if (!cf.getKey().equals(RocksDataBase.DEFAULT_COLUMN_FAMILY)) {
                columnFamilies[pos++] = cf.getKey();
            }
        }

        return columnFamilies;
    }

    @Override
    public void createColumnFamily(String name) throws DataSourceDatabaseException {
        try {
            rocksDataBase.createColumnFamily(name);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public void dropColumnFamily(String name) throws DataSourceDatabaseException {
        try {
            rocksDataBase.dropColumnFamily(name);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public boolean containsSequence(String name) {
        return (sequenceManager.getSequence(name) != null);
    }

    @Override
    public void createSequence(String name) throws DataSourceDatabaseException {
        try {
            sequenceManager.createSequence(name);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public void dropSequence(String name) throws DataSourceDatabaseException {
        try {
            sequenceManager.dropSequence(name);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }
}
