package com.infomaximum.rocksdb.transaction;

import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.exeption.TransactionNotFoundException;
import com.infomaximum.rocksdb.RocksDataBase;
import com.infomaximum.rocksdb.RocksDataBaseBuilder;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.core.datasource.RocksDBDataSourceImpl;
import org.junit.Assert;
import org.junit.Test;

public class TransactionTest extends RocksDataTest {

    @Test
    public void beginThanCommit() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DataSource dataSource = new RocksDBDataSourceImpl(rocksDataBase);

            long transactionId = dataSource.beginTransaction();
            dataSource.commitTransaction(transactionId);

            try {
                dataSource.commitTransaction(transactionId);
            } catch (TransactionNotFoundException e) {
                Assert.assertTrue(true);
                return;
            }
        }

        Assert.fail();
    }

    @Test
    public void beginThanRollback() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DataSource dataSource = new RocksDBDataSourceImpl(rocksDataBase);

            long transactionId = dataSource.beginTransaction();
            dataSource.rollbackTransaction(transactionId);

            try {
                dataSource.rollbackTransaction(transactionId);
            } catch (TransactionNotFoundException e) {
                Assert.assertTrue(true);
                return;
            }
        }

        Assert.fail();
    }

    @Test
    public void commit() throws Exception {
        try (RocksDataBase rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build()) {
            DataSource dataSource = new RocksDBDataSourceImpl(rocksDataBase);

            long transactionId = dataSource.beginTransaction();
            dataSource.commitTransaction(transactionId);

            try {
                dataSource.commitTransaction(transactionId);
            } catch (TransactionNotFoundException e) {
                Assert.assertTrue(true);
                return;
            }
        }

        Assert.fail();
    }
}
