package com.infomaximum.rocksdb.test.domain.iterator;

import com.google.common.primitives.Longs;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.rocksdb.domain.StoreFileEditable;
import com.infomaximum.rocksdb.domain.StoreFileReadable;
import com.infomaximum.rocksdb.test.StoreFileDataTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PrefixIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void findByOneField() throws Exception {
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

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр Александров");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Реализация услуг акт Бухгалтерия предприятия, редакция рукпыва р р");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Иван Васильев");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Василий Иванов");
            transaction.save(obj);
        });

        final PrefixIndexFilter filter = new PrefixIndexFilter(StoreFileReadable.FIELD_FILE_NAME, "");

        filter.setFieldValue("ghbdtn");
        testFind(filter);

        filter.setFieldValue("привет");
        testFind(filter, 1, 2 ,3, 5);

        filter.setFieldValue("вс");
        testFind(filter, 1, 3, 4, 5);

        filter.setFieldValue("вс com");
        testFind(filter, 3);

        filter.setFieldValue(".");
        testFind(filter, 5);

        filter.setFieldValue("прив info");
        testFind(filter, 3);

        filter.setFieldValue("алекс");
        testFind(filter, 6);

        filter.setFieldValue("алекс алекс");
        testFind(filter, 6);

        filter.setFieldValue("Р р");
        testFind(filter, 7);

        filter.setFieldValue("Р");
        testFind(filter, 7);

        filter.setFieldValue("Ре");
        testFind(filter, 7);

        filter.setFieldValue("вас");
        testFind(filter, 8, 9);
    }

    @Test
    public void findByTwoField() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет всем");
            obj.setContentType("test1");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("привет");
            obj.setContentType("test2 test21");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("ПРИВЕТ ВСЕМ info.com");
            obj.setContentType("test3 3test");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("всем");
            obj.setContentType("4test");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("прИВет всЕм .dor");
            obj.setContentType("5test rest");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр");
            obj.setContentType("Александров");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Реализация услуг акт Бухгалтерия ");
            obj.setContentType("предприятия, редакция рукпыва р р");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Александр");
            obj.setContentType("Иванов");
            transaction.save(obj);

            obj = transaction.create(StoreFileEditable.class);
            obj.setFileName("Андрей ");
            obj.setContentType("Александров");
            transaction.save(obj);
        });

        final PrefixIndexFilter filter = new PrefixIndexFilter(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE), "");

        filter.setFieldValue("ghbdtn");
        testFind(filter);

        filter.setFieldValue("привет test21");
        testFind(filter, 2);

        filter.setFieldValue("вс test");
        testFind(filter, 1, 3);

        filter.setFieldValue("test вс com");
        testFind(filter, 3);

        filter.setFieldValue("4test");
        testFind(filter, 4);

        filter.setFieldValue("5te res");
        testFind(filter, 5);

        filter.setFieldValue("алекс");
        testFind(filter, 6, 8, 9);

        filter.setFieldValue("алекс алекс");
        testFind(filter, 6);

        filter.setFieldValue("Р");
        testFind(filter, 7);

        filter.setFieldValue("Ре");
        testFind(filter, 7);
    }

    @Test
    public void findAmongBlocks() throws Exception {
        final int idCount = 3 * PrefixIndexUtils.PREFERRED_MAX_ID_COUNT_PER_BLOCK + 200;
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

        List<Long> foundIds = new ArrayList<>(temp.size());
        try (IteratorEntity<StoreFileReadable> iterator = domainObjectSource.find(StoreFileReadable.class, filter)) {
            while (iterator.hasNext()) {
                foundIds.add(iterator.next().getId());
            }
        }

        temp.sort(Long::compareTo);
        foundIds.sort(Long::compareTo);

        Assert.assertEquals(temp, foundIds);
    }
}
