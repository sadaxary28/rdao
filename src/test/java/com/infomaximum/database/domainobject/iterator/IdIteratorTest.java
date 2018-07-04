package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.filter.IdFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class IdIteratorTest extends StoreFileDataTest {

    @Test
    public void filter() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        assertFilter(1, 10, new IdFilter(0, Long.MAX_VALUE));
        assertFilter(5, 7, new IdFilter(5, 7));
        assertFilter(0, 0, new IdFilter(50, 70));
        assertFilter(1, 10, new IdFilter(1, 11));
        assertFilter(1, 1, new IdFilter(1, 1));

        domainObjectSource.executeTransactional(transaction -> {
            try (IteratorEntity<StoreFileEditable> i = domainObjectSource.find(StoreFileEditable.class, EmptyFilter.INSTANCE)) {
                while (i.hasNext()) {
                    transaction.remove(i.next());
                }
            }
        });
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        assertFilter(0, 0, new IdFilter(0, 10));
        assertFilter(11, 20, new IdFilter(5, Long.MAX_VALUE));
        assertFilter(11, 15, new IdFilter(5, 15));
        assertFilter(11, 11, new IdFilter(11, 11));
    }

    @Test
    public void loadTwoFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Set<String> loadingFields = new HashSet<>(Arrays.asList(StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_SIZE));
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new IdFilter(0), loadingFields)) {
            int iteratedRecordCount = 0;
            while (i.hasNext()) {
                StoreFileReadable storeFile = i.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }
            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    @Test
    public void loadZeroFields() throws Exception {
        final int insertedRecordCount = 10;
        initAndFillStoreFiles(domainObjectSource, insertedRecordCount);

        Set<String> loadingFields = Collections.emptySet();
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, new IdFilter(0), loadingFields)) {
            int iteratedRecordCount = 0;
            while (i.hasNext()) {
                StoreFileReadable storeFile = i.next();

                checkLoadedState(storeFile, loadingFields);

                ++iteratedRecordCount;
            }

            Assert.assertEquals(insertedRecordCount, iteratedRecordCount);
        }
    }

    private void assertFilter(final long expectedFromId, final long expectedToId, IdFilter filter) throws DatabaseException {
        long expectedRecordCount = expectedToId == expectedFromId && expectedToId == 0
                ? 0
                : (expectedToId - expectedFromId) + 1;

        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, filter)) {
            int iteratedRecordCount = 0;
            long currId = expectedFromId;
            StoreFileReadable storeFile = null;
            while (i.hasNext()) {
                storeFile = i.next();

                Assert.assertEquals(currId, storeFile.getId());
                ++iteratedRecordCount;
                ++currId;
            }
            if (storeFile != null) {
                Assert.assertEquals(expectedToId, storeFile.getId());
            }
            Assert.assertEquals(expectedRecordCount, iteratedRecordCount);
        }
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                transaction.save(obj);
            }
        });
    }
}
