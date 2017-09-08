package com.infomaximum.rocksdb.test.key;

import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.domainobject.key.KeyAvailability;
import com.infomaximum.database.domainobject.key.KeyField;
import com.infomaximum.database.domainobject.key.KeyIndex;
import com.infomaximum.util.RandomUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by kris on 24.05.17.
 */
public class KeyTest {

    @Test
    public void testAvailabilityKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());

            KeyAvailability key = new KeyAvailability(id);

            String sKey = key.pack();

            KeyAvailability checkKey = (KeyAvailability) Key.parse(sKey);
            Assert.assertEquals(id, checkKey.getId());
        }
    }

    @Test
    public void testFieldKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            String fieldName = UUID.randomUUID().toString().toLowerCase().replace("-", "");

            KeyField key = new KeyField(id, fieldName);

            String sKey = key.pack();

            KeyField checkKey = (KeyField) Key.parse(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertEquals(fieldName, checkKey.getFieldName());
        }
    }


    @Test
    public void testIndexKey() throws Exception {
        for (int i=0; i<1000; i++) {
            long id = Math.abs(RandomUtil.random.nextLong());
            String index = UUID.randomUUID().toString().toLowerCase().replace("-", "");
            int hash = Math.abs(RandomUtil.random.nextInt());

            KeyIndex key = new KeyIndex(id, index, hash);

            String sKey = key.pack();

            KeyIndex checkKey = (KeyIndex) Key.parse(sKey);
            Assert.assertEquals(id, checkKey.getId());
            Assert.assertEquals(index, checkKey.getIndex());
            Assert.assertEquals(hash, checkKey.getHash());
        }
    }
}
