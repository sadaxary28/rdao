package com.infomaximum.database.utils.key;

import com.infomaximum.database.utils.TypeConvert;
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

            Assert.assertEquals(id, FieldKey.unpackId(sKey));
            Assert.assertTrue(FieldKey.unpackBeginningObject(sKey));
        }
    }

    @Test
    public void testFieldKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            String fieldName = UUID.randomUUID().toString().toLowerCase().replace("-", "");
            byte[] fieldNameBytes = TypeConvert.pack(fieldName);

            FieldKey key = new FieldKey(id, fieldNameBytes);

            byte[] sKey = key.pack();

            Assert.assertEquals(id, FieldKey.unpackId(sKey));
            Assert.assertEquals(fieldName, FieldKey.unpackFieldName(sKey));
            Assert.assertFalse(FieldKey.unpackBeginningObject(sKey));
        }
    }


    @Test
    public void testIndexKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            long[] values = { Math.abs(RandomUtil.random.nextLong()), Math.abs(RandomUtil.random.nextLong())};

            HashIndexKey key = new HashIndexKey(id, values);

            byte[] sKey = key.pack();

            HashIndexKey checkKey = HashIndexKey.unpack(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertArrayEquals(values, checkKey.getFieldValues());
        }
    }
}
