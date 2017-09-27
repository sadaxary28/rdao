package com.infomaximum.rocksdb.core.datasource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.infomaximum.database.core.sequence.SequenceManager;
import com.infomaximum.database.core.transaction.modifier.Modifier;
import com.infomaximum.database.core.transaction.modifier.ModifierRemove;
import com.infomaximum.database.core.transaction.modifier.ModifierSet;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.IteratorNotFoundException;
import com.infomaximum.database.exeption.TransactionNotFoundException;
import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.rocksdb.RocksDataBase;
import org.rocksdb.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by user on 20.04.2017.
 */
public class RocksDBDataSourceImpl implements DataSource {

    private final long ROCKS_OBJECT_LIFE_TIME_IN_MIN = 10;

    private final RocksDataBase rocksDataBase;
    private final SequenceManager sequenceManager;

    private final AtomicLong seqRocksObject;
    private final Cache<Object, Object> iterators;
    private final Cache<Object, Object> transactions;

    class IteratorWrap {
        public final RocksIterator iterator;
        public final byte[] keyPrefix;

        public IteratorWrap(RocksIterator iterator, final byte[] keyPrefix) {
            this.iterator = iterator;
            this.keyPrefix = keyPrefix != null ? Arrays.copyOf(keyPrefix, keyPrefix.length) : null;
        }
    }

    public RocksDBDataSourceImpl(RocksDataBase rocksDataBase) throws RocksDBException {
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
    public KeyValue next(long iteratorId) throws DataSourceDatabaseException {
        IteratorWrap iter = (IteratorWrap) iterators.getIfPresent(iteratorId);
        if (iter == null) {
            throw new IteratorNotFoundException(iteratorId);
        }

        RocksIterator iterator = iter.iterator;
        try {
            if (!iterator.isValid()) {
                iterator.status();
                return null;
            }

            byte[] key = iterator.key();
            if (iter.keyPrefix != null && !ByteUtils.startsWith(iter.keyPrefix, key)) {
                return null;
            }

            KeyValue keyValue = new KeyValue(key, iterator.value());
            iterator.next();
            return keyValue;
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
        return createIterator(columnFamily, null, columnFamilyHandle -> rocksDataBase.getRocksDB().newIterator(columnFamilyHandle, rocksDataBase.getReadOptions()));
    }

    @Override
    public long createIterator(String columnFamily, final byte[] keyPrefix) throws DataSourceDatabaseException {
        return createIterator(columnFamily, keyPrefix, columnFamilyHandle -> rocksDataBase.getRocksDB().newIterator(columnFamilyHandle, rocksDataBase.getReadOptions()));
    }

    @Override
    public long createIterator(String columnFamily, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        return createIterator(columnFamily, null, columnFamilyHandle -> transaction.getIterator(rocksDataBase.getReadOptions(), columnFamilyHandle));
    }

    private long createIterator(final String columnFamily, final byte[] keyPrefix, Function<ColumnFamilyHandle, RocksIterator> iteratorGetter) {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        RocksIterator rocksIterator = iteratorGetter.apply(columnFamilyHandle);
        if (keyPrefix == null) {
            rocksIterator.seekToFirst();
        }
        else {
            rocksIterator.seek(keyPrefix);
        }

        long iteratorId = seqRocksObject.getAndIncrement();
        iterators.put(iteratorId, new IteratorWrap(rocksIterator, keyPrefix));

        return iteratorId;
    }

    @Override
    public void closeIterator(long iteratorId) {
        iterators.invalidate(iteratorId);
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
