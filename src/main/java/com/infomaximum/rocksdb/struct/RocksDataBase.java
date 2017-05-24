package com.infomaximum.rocksdb.struct;

import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.sequence.ManagerSequence;
import com.infomaximum.rocksdb.sequence.Sequence;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.rocksdb.*;

import java.util.Map;
import java.util.Set;

/**
 * Created by kris on 10.03.17.
 */
public class RocksDataBase {

    private final RocksDB rocksDB;
    private final DBOptions dbOptions;
    private final Map<String, ColumnFamilyHandle> columnFamilies;
    private final ManagerSequence managerSequence;

    public RocksDataBase(RocksDB rocksDB, DBOptions dbOptions, Map<String, ColumnFamilyHandle> columnFamilies, Set<Class<? extends DomainObject>> classMaintenancies) throws RocksDBException {
        this.rocksDB = rocksDB;
        this.dbOptions=dbOptions;
        this.columnFamilies=columnFamilies;

        this.managerSequence=new ManagerSequence(this);
    }

    public ColumnFamilyHandle getColumnFamilyHandle(String columnFamilyName) throws RocksDBException {
        ColumnFamilyHandle columnFamilyHandle = columnFamilies.get(columnFamilyName);
        if (columnFamilyHandle==null) {
            synchronized (columnFamilies) {
                columnFamilyHandle = columnFamilies.get(columnFamilyName);
                if (columnFamilyHandle==null) {
                    ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(columnFamilyName.getBytes(TypeConvertRocksdb.ROCKSDB_CHARSET));
                    columnFamilyHandle = rocksDB.createColumnFamily(columnFamilyDescriptor);
                    columnFamilies.put(columnFamilyName, columnFamilyHandle);
                }
            }
        }
        return columnFamilyHandle;
    }

    public RocksDB getRocksDB() {
        return rocksDB;
    }

    public Sequence getSequence(String sequenceName) throws RocksDBException {
        return managerSequence.getSequence(sequenceName);
    }

    public void destroy() {
        dbOptions.close();
        rocksDB.close();
    }
}
