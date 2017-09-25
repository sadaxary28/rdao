package com.infomaximum.rocksdb.core.datasource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.infomaximum.database.core.sequence.SequenceManager;
import com.infomaximum.database.core.transaction.struct.modifier.Modifier;
import com.infomaximum.database.core.transaction.struct.modifier.ModifierRemove;
import com.infomaximum.database.core.transaction.struct.modifier.ModifierSet;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.entitysource.EntitySource;
import com.infomaximum.database.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.database.domainobject.key.*;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.IteratorNotFoundException;
import com.infomaximum.database.exeption.TransactionNotFoundException;
import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDataBase;
import org.rocksdb.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by user on 20.04.2017.
 */
public class RocksDBDataSourceImpl implements DataSource {

    private final RocksDataBase rocksDataBase;
    private final SequenceManager sequenceManager;

    private final AtomicLong seqRocksObject;
    private final Cache<Object, Object> iterators;
    private final Cache<Object, Object> transactions;

    public RocksDBDataSourceImpl(RocksDataBase rocksDataBase) throws RocksDBException {
        this.rocksDataBase = rocksDataBase;
        this.sequenceManager = new SequenceManager(rocksDataBase);

        this.seqRocksObject = new AtomicLong(1);
        this.iterators = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener(notification -> {
                    RocksIterator iterator = (RocksIterator) notification.getValue();
                    iterator.close();
                })
                .build();
        this.transactions = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
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
    public byte[] getField(String columnFamily, long id, String field) throws DataSourceDatabaseException {
        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
            return rocksDataBase.getRocksDB().get(columnFamilyHandle, rocksDataBase.getReadOptions(), TypeConvert.pack(new KeyField(id, field).pack()));
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public byte[] getField(String columnFamily, long id, String field, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
            return transaction.get(columnFamilyHandle, rocksDataBase.getReadOptions(), TypeConvert.pack(new KeyField(id, field).pack()));
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }


    @Override
    public EntitySource findNextEntitySource(String columnFamily, Long prevId, String indexColumnFamily, int hash, Set<String> fields) throws DataSourceDatabaseException {
        try {
            ColumnFamilyHandle indexColumnFamilyHandle = rocksDataBase.getColumnFamilyHandle(indexColumnFamily);

            try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(indexColumnFamilyHandle)) {
                if (prevId==null) {
                    rocksIterator.seek(TypeConvert.pack(KeyIndex.prifix(hash)));
                } else {
                    rocksIterator.seek(TypeConvert.pack(new KeyIndex(prevId, hash).pack()));
                }

                while (true) {
                    if (!rocksIterator.isValid()) {
                        rocksIterator.status();
                        return null;
                    }

                    Key key = Key.parse(TypeConvert.getString(rocksIterator.key()));
                    if (key.getTypeKey() != TypeKey.INDEX) return null;

                    KeyIndex keyIndex = (KeyIndex) key;
                    if (keyIndex.getHash() != hash) return null;

                    long id = key.getId();

                    if (prevId!=null && id==prevId) {
                        rocksIterator.next();
                        continue;
                    }

                    EntitySource entitySource = getEntitySource(columnFamily, id, fields);
                    if (entitySource!=null) {
                        return entitySource;
                    } else {
                        //Сломанный индекс - этого объекта уже нет...
                        rocksIterator.next();
                    }
                }
            }
        } catch (Exception e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public EntitySource getEntitySource(String columnFamily, long id, Set<String> fields) throws DataSourceDatabaseException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        try (RocksIterator iterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle, rocksDataBase.getReadOptions())){
            return getEntitySource(iterator, id, fields);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public EntitySource getEntitySource(String columnFamily, long id, Set<String> fields, long transactionId) throws DataSourceDatabaseException {
        Transaction transaction = (Transaction) transactions.getIfPresent(transactionId);
        if (transaction == null) {
            throw new TransactionNotFoundException(transactionId);
        }

        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        try (RocksIterator iterator = transaction.getIterator(rocksDataBase.getReadOptions(), columnFamilyHandle)){
            return getEntitySource(iterator, id, fields);
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    private EntitySource getEntitySource(RocksIterator iterator, long id, Set<String> fields) throws RocksDBException {
        boolean availability = false;
        Map<String, byte[]> fieldValues = new HashMap<>(fields.size());
        iterator.seek(TypeConvert.pack(new KeyAvailability(id).pack()));
        while (true) {
            if (!iterator.isValid()) {
                iterator.status();
                break;
            }

            Key key = Key.parse(TypeConvert.getString(iterator.key()));
            if (key.getId() != id) break;

            TypeKey typeKey = key.getTypeKey();
            if (typeKey == TypeKey.AVAILABILITY) {
                availability = true;
            } else if (typeKey == TypeKey.FIELD) {
                String fieldName = ((KeyField) key).getFieldName();
                if (fields.contains(fieldName)) {
                    fieldValues.put(fieldName, iterator.value());
                }
            } else if (typeKey == TypeKey.INDEX) {
                break;
            } else {
                throw new RuntimeException("Not support type key: " + typeKey);
            }

            iterator.next();
        }

        return availability ? new EntitySourceImpl(id, fieldValues) : null;
    }

    @Override
    public EntitySource nextEntitySource(long iteratorId, Set<String> fields) throws DataSourceDatabaseException {
        RocksIterator rocksIterator = (RocksIterator) iterators.getIfPresent(iteratorId);
        if (rocksIterator == null) {
            throw new IteratorNotFoundException(iteratorId);
        }

        try {
            KeyAvailability keyAvailability = null;
            Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
            while (true) {
                if (!rocksIterator.isValid()) {
                    rocksIterator.status();
                    break;
                }

                Key key = Key.parse(TypeConvert.getString(rocksIterator.key()));
                TypeKey typeKey = key.getTypeKey();
                if (typeKey == TypeKey.AVAILABILITY) {
                    if (keyAvailability == null) {
                        keyAvailability = (KeyAvailability) key;
                    } else {
                        break;//начался следующий элемент
                    }
                }

                if (keyAvailability != null) {
                    if (typeKey == TypeKey.FIELD) {
                        String fieldName = ((KeyField) key).getFieldName();
                        if (fields.contains(fieldName)) {
                            fieldValues.put(fieldName, rocksIterator.value());
                        }
                    }
                }

                rocksIterator.next();
            }

            if (keyAvailability != null) {
                return new EntitySourceImpl(keyAvailability.getId(), fieldValues);
            } else {
                return null;
            }
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
                    transaction.put(columnFamilyHandle, TypeConvert.pack(modifier.key), modifierSet.getValue());
                } else if (modifier instanceof ModifierRemove) {
                    String key = modifier.key;
                    if (key.charAt(key.length() - 1) != '*') {
                        //Удаляется только одна запись
                        transaction.delete(columnFamilyHandle, TypeConvert.pack(key));
                    } else {
                        //Удаляются все записи попадающие под этот патерн
                        final byte[] patternKey = TypeConvert.pack(key.substring(0, key.length() - 1));
                        try (RocksIterator iterator = transaction.getIterator(rocksDataBase.getReadOptions(), columnFamilyHandle)) {
                            iterator.seek(patternKey);
                            while (true) {
                                if (!iterator.isValid()){
                                    iterator.status();
                                    break;
                                }
                                final byte[] foundKey = iterator.key();
                                if (!ByteUtils.startsWith(patternKey, foundKey)) {
                                    break;
                                }
                                transaction.delete(columnFamilyHandle, foundKey);
                                iterator.next();
                            }
                        }
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
            throw new TransactionNotFoundException(transactionId);
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

    private long createIterator(String columnFamily, Function<ColumnFamilyHandle, RocksIterator> iteratorGetter) {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        RocksIterator rocksIterator = iteratorGetter.apply(columnFamilyHandle);
        rocksIterator.seekToFirst();

        long iteratorId = seqRocksObject.getAndIncrement();
        iterators.put(iteratorId, rocksIterator);

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
