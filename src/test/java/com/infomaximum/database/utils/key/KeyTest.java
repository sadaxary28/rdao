package com.infomaximum.database.utils.key;

import com.infomaximum.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by kris on 24.05.17.
 */
public class KeyTest {

    @Test
    public void testBeginningKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());

            FieldKey key = new FieldKey(id);

            byte[] sKey = key.pack();

            FieldKey checkKey = FieldKey.unpack(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertTrue(checkKey.isBeginningObject());
        }
    }

    @Test
    public void testFieldKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            String fieldName = UUID.randomUUID().toString().toLowerCase().replace("-", "");

            FieldKey key = new FieldKey(id, fieldName);

            byte[] sKey = key.pack();

            FieldKey checkKey = FieldKey.unpack(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertEquals(fieldName, checkKey.getFieldName());
            Assert.assertFalse(checkKey.isBeginningObject());
        }
    }


    @Test
    public void testIndexKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            long[] values = { Math.abs(RandomUtil.random.nextLong()), Math.abs(RandomUtil.random.nextLong())};

            IndexKey key = new IndexKey(id, values);

            byte[] sKey = key.pack();

            IndexKey checkKey = IndexKey.unpack(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertArrayEquals(values, checkKey.getFieldValues());
        }
    }
}
