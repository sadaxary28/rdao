package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.domainobject.key.PrefixIndexKey;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrefixIndexTest extends StoreFileDataTest {

    private final String fileName = " Test sTring info@mail.com \r";
    private final List<String> lexemes = Arrays.asList("test", "string", "info@mail.com", "mail.com", "com");

    private String indexColumnFamily;

    @Before
    public void init() throws Exception {
        super.init();

        indexColumnFamily = Schema.getEntity(StoreFileReadable.class).getPrefixIndexes().get(0).columnFamily;
    }

    @Test
    public void insertWithNotOverflowedBlock() throws Exception {
        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        long iteratorId = dataSource.createIterator(indexColumnFamily, null);
        try {
            while (true) {
                KeyValue keyValue = dataSource.next(iteratorId);
                if (keyValue == null) {
                    break;
                }

                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertTrue(currentLexemes.remove(key.getLexeme()));
                Assert.assertEquals(PrefixIndexKey.FIRST_BLOCK_NUMBER, key.getBlockNumber());
                Assert.assertArrayEquals(buffer.array(), keyValue.getValue());
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void insertWithOverflowedBlock() throws Exception {
        final int recordCountForFullBlock = PrefixIndexUtils.MAX_ID_COUNT_PER_BLOCK;
        ByteBuffer bufferForFullBlock = createRecords(recordCountForFullBlock);

        final int recordCount = 10;
        ByteBuffer buffer = createRecords(recordCount);

        List<String> currentLexemes = new ArrayList<>(lexemes);
        long iteratorId = dataSource.createIterator(indexColumnFamily, null);
        try {
            while (true) {
                KeyValue keyValue = dataSource.next(iteratorId);
                if (keyValue == null) {
                    break;
                }

                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertEquals(PrefixIndexKey.FIRST_BLOCK_NUMBER - 1, key.getBlockNumber());
                Assert.assertArrayEquals(buffer.array(), keyValue.getValue());

                keyValue = dataSource.next(iteratorId);
                key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertEquals(PrefixIndexKey.FIRST_BLOCK_NUMBER, key.getBlockNumber());
                Assert.assertArrayEquals(bufferForFullBlock.array(), keyValue.getValue());

                Assert.assertTrue(currentLexemes.remove(key.getLexeme()));
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void remove() throws Exception {
        final int recordCount = 10;
        createRecords(recordCount);

        final long removingId = 7;
        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, removingId));
        });

        ByteBuffer buffer = TypeConvert.allocateBuffer((recordCount - 1) * Key.ID_BYTE_SIZE);
        long id = 0;
        for (int i = 0; i < recordCount; ++i) {
            if (++id != removingId) {
                buffer.putLong(id);
            }
        }

        List<String> currentLexemes = new ArrayList<>(lexemes);
        long iteratorId = dataSource.createIterator(indexColumnFamily, null);
        try {
            while (true) {
                KeyValue keyValue = dataSource.next(iteratorId);
                if (keyValue == null) {
                    break;
                }

                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertTrue(currentLexemes.remove(key.getLexeme()));
                Assert.assertEquals(PrefixIndexKey.FIRST_BLOCK_NUMBER, key.getBlockNumber());
                Assert.assertArrayEquals(buffer.array(), keyValue.getValue());
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    @Test
    public void update() throws Exception {
        final int recordCount = 1;
        ByteBuffer buffer = createRecords(recordCount);

        final String newFileName = " Test sTrIng inform@mail. \r";
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, 1);
            obj.setFileName(newFileName);
            transaction.save(obj);
        });

        List<String> currentLexemes = new ArrayList<>(Arrays.asList("test", "string", "inform@mail.", "mail."));
        long iteratorId = dataSource.createIterator(indexColumnFamily, null);
        try {
            while (true) {
                KeyValue keyValue = dataSource.next(iteratorId);
                if (keyValue == null) {
                    break;
                }

                PrefixIndexKey key = PrefixIndexKey.unpack(keyValue.getKey());
                Assert.assertTrue(currentLexemes.remove(key.getLexeme()));
                Assert.assertEquals(PrefixIndexKey.FIRST_BLOCK_NUMBER, key.getBlockNumber());
                Assert.assertArrayEquals(buffer.array(), keyValue.getValue());
            }
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        Assert.assertEquals(0, currentLexemes.size());
    }

    private ByteBuffer createRecords(int recordCount) throws Exception {
        ByteBuffer idsBuffer = TypeConvert.allocateBuffer(recordCount * Key.ID_BYTE_SIZE);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName(fileName);
                transaction.save(obj);
                idsBuffer.putLong(obj.getId());
            }
        });

        return idsBuffer;
    }
}
