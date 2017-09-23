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
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by user on 20.04.2017.
 */
public class RocksDBDataSourceImpl implements DataSource {

    private final RocksDataBase rocksDataBase;
    private final SequenceManager sequenceManager;

    private final AtomicLong seqIterator;
    private final Cache<Object, Object> iterators;


    public RocksDBDataSourceImpl(RocksDataBase rocksDataBase) throws RocksDBException {
        this.rocksDataBase = rocksDataBase;
        this.sequenceManager = new SequenceManager(rocksDataBase);

        this.iterators = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener(notification -> {
                    RocksIterator rocksIterator = (RocksIterator) notification.getValue();
                    rocksIterator.close();
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
            return rocksDataBase.getRocksDB().get(columnFamilyHandle, TypeConvert.pack(new KeyField(id, field).pack()));
        } catch (Exception e) {
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
                    if (!rocksIterator.isValid()) return null;

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
        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);

            boolean availability = false;
            Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
            try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
                rocksIterator.seek(TypeConvert.pack(new KeyAvailability(id).pack()));
                while (true) {
                    if (!rocksIterator.isValid()) break;

                    Key key = Key.parse(TypeConvert.getString(rocksIterator.key()));
                    if (key.getId() != id) break;

                    TypeKey typeKey = key.getTypeKey();
                    if (typeKey == TypeKey.AVAILABILITY) {
                        availability = true;
                    } else if (typeKey == TypeKey.FIELD) {
                        String fieldName = ((KeyField) key).getFieldName();
                        if (fields.contains(fieldName)) {
                            fieldValues.put(fieldName, rocksIterator.value());
                        }
                    } else if (typeKey == TypeKey.INDEX) {
                        break;
                    } else {
                        throw new RuntimeException("Not support type key: " + typeKey);
                    }

                    rocksIterator.next();
                }
            }

            if (availability) {
                return new EntitySourceImpl(id, fieldValues);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public EntitySource nextEntitySource(long iteratorId, Long prevId, Set<String> fields) throws DataSourceDatabaseException {
        try {
            RocksIterator rocksIterator = (RocksIterator) iterators.getIfPresent(iteratorId);
            if (rocksIterator == null) throw new IteratorDataSourceDatabaseException("iterator " + iteratorId + " not found");

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
        } catch (Exception e) {
            throw new DataSourceDatabaseException(e);
        }
    }

    @Override
    public void commit(List<Modifier> modifiers) throws DataSourceDatabaseException {
        try {
            try(WriteBatch writeBatch = new WriteBatch()) {
                for (Modifier modifier : modifiers) {
                    ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(modifier.columnFamily);

                    if (modifier instanceof ModifierSet) {
                        ModifierSet modifierSet = (ModifierSet) modifier;
                        writeBatch.put(columnFamilyHandle, TypeConvert.pack(modifier.key), modifierSet.getValue());
                    } else if (modifier instanceof ModifierRemove) {
                        String key = modifier.key;
                        if (key.charAt(key.length() - 1) != '*') {
                            //Удаляется только одна запись
                            writeBatch.remove(columnFamilyHandle, TypeConvert.pack(key));
                        } else {
                            //Удаляются все записи попадающие под этот патерн
                            String patternKey = key.substring(0, key.length() - 1);
                            try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
                                rocksIterator.seek(TypeConvert.pack(patternKey));
                                while (true) {
                                    if (!rocksIterator.isValid()) break;
                                    byte[] findKey = rocksIterator.key();
                                    String sFindKey = TypeConvert.getString(findKey);
                                    if (sFindKey.startsWith(patternKey)) {
                                        writeBatch.remove(columnFamilyHandle, findKey);
                                    } else {
                                        break;
                                    }
                                    rocksIterator.next();
                                }
                            }
                        }
                    } else {
                        throw new RuntimeException("Not support type modifier: " + modifier.getClass());
                    }
                }

                //Коммитим
                try( WriteOptions writeOptions = new WriteOptions() ) {
                    writeOptions.setSync(true);
                    rocksDataBase.getRocksDB().write(writeOptions, writeBatch);
                }
            }
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }
    }


    @Override
    public long createIterator(String columnFamily) throws DataSourceDatabaseException {
        RocksIterator rocksIterator;
        try {
            ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
            rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle);
            rocksIterator.seekToFirst();
        } catch (RocksDBException e) {
            throw new DataSourceDatabaseException(e);
        }

        long iteratorId = seqIterator.getAndIncrement();
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
