package com.infomaximum.rocksdb.core.datasource;

import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.engine.impl.EngineTransactionImpl;
import com.infomaximum.rocksdb.transaction.engine.impl.TransactionImpl;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.HashMap;
import java.util.Map;

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
    public Map<String, byte[]> load(String columnFamily, long id, boolean isReadOnly) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = rocksDataBase.getColumnFamilyHandle(columnFamily);
        RocksIterator rocksIterator = rocksDataBase.getRocksDB().newIterator(columnFamilyHandle);

        Map<String, byte[]> values = new HashMap<String, byte[]>();

        rocksIterator.seek(TypeConvertRocksdb.pack(id));
        while (true) {
            if (!rocksIterator.isValid()) break;

            Object[] keySplit = DomainObjectUtils.parseRocksDBKey(TypeConvertRocksdb.getString(rocksIterator.key()));
            long iID = (long) keySplit[0];
            if (iID!=id) break;
            String fieldName = (String) keySplit[1];

            values.put(fieldName, rocksIterator.value());
            rocksIterator.next();
        }

        if (values.isEmpty()) {
            return null;
        } else {
            return values;
        }
    }

    @Override
    public void set(String columnFamily, long id, String field, byte[] value) {

    }
}
