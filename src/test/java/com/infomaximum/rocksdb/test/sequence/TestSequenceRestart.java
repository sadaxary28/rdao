package com.infomaximum.rocksdb.test.sequence;

import com.infomaximum.database.core.sequence.Sequence;
import com.infomaximum.rocksdb.RocksDataTest;
import com.infomaximum.rocksdb.builder.RocksdbBuilder;
import com.infomaximum.rocksdb.struct.RocksDataBase;
import com.infomaximum.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by kris on 22.04.17.
 */
public class TestSequenceRestart extends RocksDataTest {

    private final static Logger log = LoggerFactory.getLogger(TestSequenceRestart.class);

    @Test
    public void run() throws Exception {
        String sequenceName = "sdfuisii";
        Set<Long> ids = new HashSet<Long>();

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
            startDbAndIncrementSequence(sequenceName, ids, RandomUtil.random.nextInt(100)+1);
        }
    }

    private void startDbAndIncrementSequence(String sequenceName, Set<Long> ids, int count) throws Exception {
        RocksDataBase rocksDataBase = new RocksdbBuilder()
                .withPath(pathDataBase)
                .build();

        Sequence sequence = rocksDataBase.getSequence(sequenceName);
        for (int i=0; i < count; i++) {
            checkAndAddId(ids, sequence.next());
        }

        rocksDataBase.destroy();
    }

    private void checkAndAddId(Set<Long> ids, long id) {
        if (ids.contains(id)) Assert.fail("Conflict gen id");
        ids.add(id);
    }
}
