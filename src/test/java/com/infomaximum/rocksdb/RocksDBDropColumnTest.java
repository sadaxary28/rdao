package com.infomaximum.rocksdb;

import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.utils.TypeConvert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDBDropColumnTest extends RocksDataTest {

    @Test
    public void dropColumnFamily() throws Exception {
        String cfName = "com.infomaximum.store.StoreFile.index.size:java.lang.Long";

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            rocksDBProvider.createColumnFamily(cfName);

            try (DBTransaction transaction = rocksDBProvider.beginTransaction()) {
                transaction.put(cfName, "key".getBytes(), "value".getBytes());
                transaction.commit();
            }
        }

        try (RocksDBProvider rocksDBProvider = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            rocksDBProvider.dropColumnFamily(cfName);
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }

    @Test
    public void dropColumnFamilyNative() throws Exception {
        final byte[] cfName = "test".getBytes();

        // create DB with new column family
        try(final Options options = new Options().setCreateIfMissing(true);
            final OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString());
            final WriteOptions writeOptions = new WriteOptions()) {

            try (ColumnFamilyHandle cf = txnDb.getBaseDB().createColumnFamily(new ColumnFamilyDescriptor(cfName))) {
                txnDb.getBaseDB().put(cf, writeOptions, "key1".getBytes(), "value1".getBytes());
                txnDb.getBaseDB().put(cf, writeOptions, "key2".getBytes(), "value2".getBytes());

                try (final Transaction txn = txnDb.beginTransaction(writeOptions)) {
                    txn.put(cf, "key3".getBytes(), "value3".getBytes());
                    txn.commit();
                }
            }
        }

        // drop column family
        List<ColumnFamilyDescriptor> desc = new ArrayList<>();
        try(final Options options = new Options().setCreateIfMissing(true)) {
            int cfIndex = -1;
            for (byte[] columnFamilyName : RocksDB.listColumnFamilies(options, pathDataBase.toString())) {
                desc.add(new ColumnFamilyDescriptor(columnFamilyName));
            }
            if (desc.isEmpty()) {
                desc.add(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));
            }
            for (int i = 0; i < desc.size(); ++i) {
                if(Arrays.equals(desc.get(i).columnFamilyName(), cfName)) {
                    cfIndex = i;
                    break;
                }
            }

            List<ColumnFamilyHandle> cfs = new ArrayList<>();
            try (OptimisticTransactionDB txnDb = OptimisticTransactionDB.open(options, pathDataBase.toString(), desc, cfs)) {
               txnDb.getBaseDB().dropColumnFamily(cfs.get(cfIndex));
                cfs.forEach(columnFamilyHandle -> columnFamilyHandle.close());
            }
        }

        FileUtils.deleteDirectory(pathDataBase.toAbsolutePath().toFile());
    }
}
