package com.infomaximum.rocksdb;

import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.utils.TypeConvert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class RocksDBDataSourceTest extends RocksDataTest {

    private RocksDataBase rocksDataBase;
    private RocksDBDataSource dataSource;

    private static final String columnFamily = "test_cf";
    private static final long startValue = 0x8000000080000000L;
    private static final int valueCount = 100;

    @Before
    public void init() throws Exception {
        super.init();

        rocksDataBase = new RocksDataBaseBuilder().withPath(pathDataBase).build();
        dataSource = new RocksDBDataSource(rocksDataBase);
    }

    @After
    public void destroy() throws Exception {
        rocksDataBase.close();

        super.destroy();
    }

    @Test
    public void seekWithStrictMatching() throws Exception {
        fillData();

        long iteratorId = dataSource.createIterator(columnFamily);
        try {
            KeyValue keyValue = dataSource.seek(iteratorId, new KeyPattern(TypeConvert.pack(0x6000000080000000L), true));
            Assert.assertNull(keyValue);

            byte[] pattern = TypeConvert.pack(0x6000000080000001L);
            keyValue = dataSource.seek(iteratorId, new KeyPattern(pattern, true));
            Assert.assertArrayEquals(pattern, keyValue.getKey());

            keyValue = dataSource.seek(iteratorId, new KeyPattern(TypeConvert.pack(0x7000000080000000L), false));
            Assert.assertArrayEquals(TypeConvert.pack(0x7000000080000001L), keyValue.getKey());

            pattern = TypeConvert.pack(0x6000000080000001L);
            keyValue = dataSource.seek(iteratorId, new KeyPattern(pattern, false));
            Assert.assertArrayEquals(pattern, keyValue.getKey());
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }

    @Test
    public void step() throws Exception {
        fillData();

        // test iterate by previous
        long iteratorId = dataSource.createIterator(columnFamily);
        try {
            dataSource.seek(iteratorId, new KeyPattern(TypeConvert.pack(0xA0000000)));
            for (long i = startValue + valueCount - 1; i >= startValue; --i) {
                KeyValue keyValue = dataSource.step(iteratorId, DataSource.StepDirection.BACKWARD);
                Assert.assertEquals(i, TypeConvert.unpackLong(keyValue.getKey()).longValue());
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        // test iterate by next
        iteratorId = dataSource.createIterator(columnFamily);
        try {
            dataSource.seek(iteratorId, new KeyPattern(TypeConvert.pack(0x70000000)));
            for (long i = startValue; i < startValue + valueCount; ++i) {
                KeyValue keyValue = dataSource.step(iteratorId, DataSource.StepDirection.FORWARD);
                Assert.assertEquals(i, TypeConvert.unpackLong(keyValue.getKey()).longValue());
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        iteratorId = dataSource.createIterator(columnFamily);
        try {
            final KeyValue firstKeyValue = dataSource.seek(iteratorId, null);
            Assert.assertArrayEquals(TypeConvert.pack(0x6000000080000001L), firstKeyValue.getKey());

            KeyValue keyValue = dataSource.step(iteratorId, DataSource.StepDirection.BACKWARD);
            Assert.assertNull(keyValue);

            final KeyValue lastKeyValue = dataSource.seek(iteratorId, new KeyPattern(TypeConvert.pack(0xA0000000)));
            Assert.assertArrayEquals(TypeConvert.pack(0xA000000080000001L), lastKeyValue.getKey());

            keyValue = dataSource.step(iteratorId, DataSource.StepDirection.FORWARD);
            Assert.assertNull(keyValue);
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }

    private void fillData() throws Exception {
        dataSource.createColumnFamily(columnFamily);

        long transactionId = dataSource.beginTransaction();
        try {
            List<Modifier> modifiers = new ArrayList<>();
            modifiers.add(new ModifierSet(columnFamily, TypeConvert.pack(0x6000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY));
            modifiers.add(new ModifierSet(columnFamily, TypeConvert.pack(0x7000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY));
            modifiers.add(new ModifierSet(columnFamily, TypeConvert.pack(0xA000000080000001L), TypeConvert.EMPTY_BYTE_ARRAY));

            for (int i = 0; i < valueCount; i++) {
                modifiers.add(new ModifierSet(columnFamily, TypeConvert.pack(startValue + i), TypeConvert.EMPTY_BYTE_ARRAY));
            }

            dataSource.modify(modifiers, transactionId);
        } finally {
            dataSource.commitTransaction(transactionId);
        }
    }
}
