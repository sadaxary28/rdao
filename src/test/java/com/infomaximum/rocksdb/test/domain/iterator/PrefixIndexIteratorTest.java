package com.infomaximum.rocksdb.test.domain.iterator;

import com.google.common.primitives.Longs;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PrefixIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void find() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ info.com");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм .dor");
            transaction.save(obj);
        });

        final PrefixIndexFilter filter = new PrefixIndexFilter(StoreFileReadable.FIELD_FILE_NAME, "");

        filter.setFieldValue("ghbdtn");
        testFind(filter);

        filter.setFieldValue("привет");
        testFind(filter, 1, 2 ,3, 5);

        filter.setFieldValue("вс");
        testFind(filter, 1, 3, 4, 5);

        filter.setFieldValue("com");
        testFind(filter, 3);

        filter.setFieldValue(".");
        testFind(filter, 5);

        filter.setFieldValue("прив info");
        testFind(filter, 3);
    }

    @Test
    public void findAmongBlocks() throws Exception {
        final int idCount = 3 * PrefixIndexUtils.MAX_ID_COUNT_PER_BLOCK + 200;
        final List<Long> expectedIds = new ArrayList<>(idCount);
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < idCount; ++i) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFileName("ПРИВЕТ ВСЕМ info.com");
                transaction.save(obj);

                expectedIds.add(obj.getId());
            }
        });

        final PrefixIndexFilter filter = new PrefixIndexFilter(StoreFileReadable.FIELD_FILE_NAME, "всем");

        testFind(filter, expectedIds);
    }

    private void testFind(PrefixIndexFilter filter, long... expectedIds) throws DatabaseException {
        testFind(filter, Longs.asList(expectedIds));
    }

    private void testFind(PrefixIndexFilter filter, List<Long> expectedIds) throws DatabaseException {
        List<Long> temp = new ArrayList<>(expectedIds);
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter)) {
            while (iterator.hasNext()) {
                StoreFileReadable obj = iterator.next();
                Assert.assertTrue(temp.remove(obj.getId()));
            }
            Assert.assertEquals(0, temp.size());
        }
    }
}
