package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.key.*;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.struct.modifier.Modifier;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierRemove;
import com.infomaximum.rocksdb.transaction.struct.modifier.ModifierSet;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by user on 20.04.2017.
 */
public class DataSourceImpl implements DataSource {

    private final static Logger log = LoggerFactory.getLogger(DataSourceImpl.class);

    private final RocksDataBase rocksDataBase;

    public DataSourceImpl(RocksDataBase rocksDataBase) {
        this.rocksDataBase = rocksDataBase;
    }

    @Override
    public long nextId(String sequenceName) throws RocksDBException {
        return rocksDataBase.getSequence(sequenceName).next();
    }

    @Override
    public byte[] getField(String columnFamily, long id, String field) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        return rocksDataBase.getRocksDB().get(columnFamilyHandle, TypeConvertRocksdb.pack(new KeyField(id, field).pack()));
    }


    @Override
    public EntitySource findNextEntitySource(String columnFamily, Long prevId, String index, int hash, Set<String> fields) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);

        try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
            if (prevId==null) {
                rocksIterator.seek(TypeConvertRocksdb.pack(KeyIndex.prifix(index, hash)));
            } else {
                rocksIterator.seek(TypeConvertRocksdb.pack(new KeyIndex(prevId, index, hash).pack()));
            }

            while (true) {
                if (!rocksIterator.isValid()) return null;

                Key key = Key.parse(TypeConvertRocksdb.getString(rocksIterator.key()));
                if (key.getTypeKey() != TypeKey.INDEX) return null;

                KeyIndex keyIndex = (KeyIndex) key;
                if (!keyIndex.getIndex().equals(index)) return null;
                if (keyIndex.getHash() != hash) return null;

                long id = key.getId();

                if (prevId!=null && id==prevId) {
                    rocksIterator.next();
                    continue;
                }

                EntitySource entitySource = getEntitySource(columnFamily, false, id, fields);
                if (entitySource!=null) {
                    return entitySource;
                } else {
                    //Сломанный индекс - этого объекта уже нет...
                    rocksIterator.next();
                }
            }
        }
    }

    @Override
    public EntitySource getEntitySource(String columnFamily, boolean isTransaction, long id, Set<String> fields) throws RocksDBException {
        if (isTransaction) {
            //TODO надо лочить объект!
        }

        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);

        boolean availability = false;
        Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
        try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
            rocksIterator.seek(TypeConvertRocksdb.pack(new KeyAvailability(id).pack()));
            while (true) {
                if (!rocksIterator.isValid()) break;

                Key key = Key.parse(TypeConvertRocksdb.getString(rocksIterator.key()));
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
    }

    @Override
    public EntitySource nextEntitySource(String columnFamily, Long prevId, Set<String> fields) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);

        KeyAvailability keyAvailability=null;
        Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
        try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
            if (prevId==null) {
                rocksIterator.seekToFirst();
            } else {
                rocksIterator.seek(TypeConvertRocksdb.pack(new KeyAvailability(prevId).pack()));
            }
            while (true) {
                if (!rocksIterator.isValid()) break;

                Key key = Key.parse(TypeConvertRocksdb.getString(rocksIterator.key()));
                TypeKey typeKey = key.getTypeKey();
                if (keyAvailability == null) {
                    if (typeKey == TypeKey.AVAILABILITY) {
                        if (prevId==null) {
                            keyAvailability = (KeyAvailability) key;
                        } else if (key.getId() != prevId) {
                            keyAvailability = (KeyAvailability) key;
                        }
                    }
                }

                if (keyAvailability!=null) {
                    if (typeKey == TypeKey.FIELD) {
                        String fieldName = ((KeyField) key).getFieldName();
                        if (fields.contains(fieldName)) {
                            fieldValues.put(fieldName, rocksIterator.value());
                        }
                    }
                }

                rocksIterator.next();
            }
        }

        if (keyAvailability!=null) {
            return new EntitySourceImpl(keyAvailability.getId(), fieldValues);
        } else {
            return null;
        }
    }

    @Override
    public void commit(List<Modifier> modifiers) throws RocksDBException {
        try {
            try(WriteBatch writeBatch = new WriteBatch()) {
                for (Modifier modifier : modifiers) {
                    ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(modifier.columnFamily);

                    if (modifier instanceof ModifierSet) {
                        ModifierSet modifierSet = (ModifierSet) modifier;
                        writeBatch.put(columnFamilyHandle, TypeConvertRocksdb.pack(modifier.key), modifierSet.getValue());
                    } else if (modifier instanceof ModifierRemove) {
                        String key = modifier.key;
                        if (key.charAt(key.length() - 1) != '*') {
                            //Удаляется только одна запись
                            writeBatch.remove(columnFamilyHandle, TypeConvertRocksdb.pack(key));
                        } else {
                            //Удаляются все записи попадающие под этот патерн
                            String patternKey = key.substring(0, key.length() - 1);
                            try (RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle)) {
                                rocksIterator.seek(TypeConvertRocksdb.pack(patternKey));
                                while (true) {
                                    if (!rocksIterator.isValid()) break;
                                    byte[] findKey = rocksIterator.key();
                                    String sFindKey = TypeConvertRocksdb.getString(findKey);
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
            log.error("Error commit, modifiers: [{}]", modifiers.stream().map(Object::toString).collect(Collectors.joining(", ")), e);
            throw e;
        }
    }

}
