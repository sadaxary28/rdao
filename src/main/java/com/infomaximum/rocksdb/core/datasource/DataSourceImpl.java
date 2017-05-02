package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.key.Key;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyAvailability;
import com.infomaximum.rocksdb.core.objectsource.utils.key.KeyField;
import com.infomaximum.rocksdb.core.objectsource.utils.key.TypeKey;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.impl.TransactionImpl;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by user on 20.04.2017.
 */
public class DataSourceImpl implements DataSource {

    private final RocksDataBase rocksDataBase;

    public DataSourceImpl(RocksDataBase rocksDataBase) {
        this.rocksDataBase = rocksDataBase;
    }

    @Override
    public Transaction createTransaction() {
        return new TransactionImpl(rocksDataBase);
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
    public EntitySource getObject(String columnFamily, long id, Set<String> fields) throws RocksDBException {
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
    public EntitySource lockObject(String columnFamily, long id, Set<String> fields) throws RocksDBException {
        //TODO надо лочить объект!
        return getObject(columnFamily, id, fields);
    }

    @Override
    public EntitySource next(String columnFamily, Long prevId, Set<String> fields) throws RocksDBException {
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

}
