package com.infomaximum.rocksdb.test.sequence;

import com.infomaximum.database.core.sequence.Sequence;
import com.infomaximum.database.core.sequence.SequenceDBException;
import com.infomaximum.database.core.sequence.SequenceManager;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class SequenceTest extends RocksDataTest {

    @Test
    public void createNew() throws Exception {
        String sequenceName = "sdfuisii";

        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDataBase);
            sequenceManager.createSequence(sequenceName);

            byte[] value = rocksDataBase.getRocksDB().get(rocksDataBase.getDefaultColumnFamily(), TypeConvert.pack(SequenceManager.SEQUENCE_PREFIX + sequenceName));
            Assert.assertTrue(TypeConvert.getLong(value) > 0);
        }
    }

    @Test
    public void createExisting() throws Exception {
        String sequenceName = "sdfuisii";

        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDataBase);
            sequenceManager.createSequence(sequenceName);

            try {
                sequenceManager.createSequence(sequenceName);
            } catch (SequenceDBException e) {
                Assert.assertTrue(true);
                return;
            }
        }

        Assert.fail();
    }

    @Test
    public void drop() throws Exception {
        String sequenceName = "sdfuisii";

        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDataBase);
            sequenceManager.createSequence(sequenceName);

            sequenceManager.dropSequence(sequenceName);
            byte[] value = rocksDataBase.getRocksDB().get(rocksDataBase.getDefaultColumnFamily(), TypeConvert.pack(SequenceManager.SEQUENCE_PREFIX + sequenceName));
            Assert.assertNull(value);
        }
    }

    @Test
    public void order() throws Exception {
        String sequenceName = "sdfuisii";

        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDataBase);
            sequenceManager.createSequence(sequenceName);

            Sequence sequence = sequenceManager.getSequence(sequenceName);
            for (int i = 1; i < 1000000; i++) {
                long id = sequence.next();
                Assert.assertEquals(i, id);
            }
        }
    }

    @Test
    public void restart() throws Exception {
        String sequenceName = "sdfuisii";
        Set<Long> ids = new HashSet<>();

        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            new SequenceManager(rocksDataBase).createSequence(sequenceName);
        }

        startDbAndIncrementSequence(sequenceName, ids, 1);
        startDbAndIncrementSequence(sequenceName, ids, 3);
        startDbAndIncrementSequence(sequenceName, ids, 0);
        startDbAndIncrementSequence(sequenceName, ids, 7);
        startDbAndIncrementSequence(sequenceName, ids, 9);
        startDbAndIncrementSequence(sequenceName, ids, 10);
        startDbAndIncrementSequence(sequenceName, ids, 11);
        startDbAndIncrementSequence(sequenceName, ids, 99);
        startDbAndIncrementSequence(sequenceName, ids, 100);
        startDbAndIncrementSequence(sequenceName, ids, 101);
        for (int i=0; i<10; i++) {
            startDbAndIncrementSequence(sequenceName, ids, RandomUtil.random.nextInt(100) + 1);
        }
    }

    private void startDbAndIncrementSequence(String sequenceName, Set<Long> ids, int count) throws Exception {
        try (RocksDataBase rocksDataBase = new RocksdbBuilder().withPath(pathDataBase).build()) {
            SequenceManager sequenceManager = new SequenceManager(rocksDataBase);

            Sequence sequence = sequenceManager.getSequence(sequenceName);
            for (int i = 0; i < count; i++) {
                checkAndAddId(ids, sequence.next());
            }
        }
    }

    private static void checkAndAddId(Set<Long> ids, long id) {
        Assert.assertFalse("Conflict gen id", ids.contains(id));
        ids.add(id);
    }
}
