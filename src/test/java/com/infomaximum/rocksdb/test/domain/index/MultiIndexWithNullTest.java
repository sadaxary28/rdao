package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.filter.IndexFilter;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

public class MultiIndexWithNullTest extends StoreFileDataTest {

    @Test
    public void findByComboIndex() throws Exception {
        final int recordCount = 100;

        domainObjectSource.executeTransactional(transaction -> {
            for (long size = 0; size < recordCount; size++) {
                StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
                storeFile.setSize(size);
                storeFile.setFileName(null);
                transaction.save(storeFile);
            }
        });

        for (long size = 0; size < recordCount; size++) {
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class,
                    new IndexFilter(StoreFileReadable.FIELD_SIZE, size).appendField(StoreFileReadable.FIELD_FILE_NAME, null))) {
                Assert.assertTrue(i.hasNext());

                StoreFileReadable storeFileReadable = i.next();

                Assert.assertEquals(size, storeFileReadable.getSize());
                Assert.assertEquals(null, storeFileReadable.getFileName());

                Assert.assertFalse(i.hasNext());
            }
        }
    }
}
