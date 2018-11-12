package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDBProvider;
import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.Transaction;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NativeTransactionFailsAtConflictTest {

    private static List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(new ColumnFamilyDescriptor(TypeConvert.pack(RocksDBProvider.DEFAULT_COLUMN_FAMILY)));

    @Test
    public void test() throws Exception {
        int fillCount = 1_500_000;

        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try(OptimisticTransactionDB db = openDB(Paths.get(new File("C:\\testDB").toURI()), columnFamilyHandles)) {
            try (Transaction transaction = db.beginTransaction(new WriteOptions())) {
                for (int i = 0; i < fillCount; i++) {
                    transaction.put(("key:" + i).getBytes(), ("value:" + i).getBytes());
                }
                transaction.commit();
            }
        }

        try(OptimisticTransactionDB db = openDB(Paths.get(new File("C:\\testDB").toURI()), columnFamilyHandles)) {
            try (Transaction transaction = db.beginTransaction(new WriteOptions())) {
                transaction.delete(("key:" + 0).getBytes());
                transaction.commit();
            }
        }
    }

    private OptimisticTransactionDB openDB(Path path, List<ColumnFamilyHandle> columnFamilyHandles) throws DatabaseException {
        columnFamilyHandles.clear();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true)) {
            return OptimisticTransactionDB.open(options, path.toString(), columnFamilyDescriptors, columnFamilyHandles);
        } catch (RocksDBException e) {
            throw new DatabaseException(e);
        }
    }
}
