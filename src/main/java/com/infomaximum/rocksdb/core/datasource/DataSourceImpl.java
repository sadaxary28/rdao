package com.infomaximum.rocksdb.core.datasource;

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
    public byte[] get(String columnFamily, long id, String field) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        return rocksDataBase.getRocksDB().get(columnFamilyHandle, TypeConvertRocksdb.pack(new KeyField(id, field).pack()));
    }

    @Override
    public Map<String, byte[]> gets(String columnFamily, long id, Set<String> fields) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);

//        if (rocksDataBase.getRocksDB().get(columnFamilyHandle, TypeConvertRocksdb.pack(new KeyAvailability(id).pack()))==null) {
//            return null;
//        } else {
//            Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
//            for (String field: fields){
//                KeyField keyField = new KeyField(id, field);
//                fieldValues.put(
//                        field,
//                        rocksDataBase.getRocksDB().get(columnFamilyHandle, TypeConvertRocksdb.pack(keyField.pack()))
//                );
//            }
//            return fieldValues;
//        }


        //TODO переписать на итератор
        RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle);
        boolean availability = false;
        Map<String, byte[]> fieldValues = new HashMap<String, byte[]>();
        rocksIterator.seek(TypeConvertRocksdb.pack(new KeyAvailability(id).pack()));
        while (true) {
            if (!rocksIterator.isValid()) break;

            Key key = Key.parse(TypeConvertRocksdb.getString(rocksIterator.key()));
            if (key.getId()!=id) break;

            TypeKey typeKey = key.getTypeKey();
            if (typeKey == TypeKey.AVAILABILITY) {
                availability = true;
            } else if (typeKey == TypeKey.FIELD) {
                String fieldName = ((KeyField)key).getFieldName();
                if (fields.contains(fieldName)) {
                    fieldValues.put(fieldName, rocksIterator.value());
                }
            } else {
                throw new RuntimeException("Not support type key: " + typeKey);
            }

            rocksIterator.next();
        }

        if (availability) {
            return fieldValues;
        } else {
            return null;
        }
    }

    @Override
    public Map<String, byte[]> lock(String columnFamily, long id, Set<String> fields) throws RocksDBException {
        //TODO надо лочить объект!
        return gets(columnFamily, id, fields);
    }

}
