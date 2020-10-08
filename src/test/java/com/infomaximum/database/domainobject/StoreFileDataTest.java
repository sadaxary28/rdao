package com.infomaximum.database.domainobject;

import com.google.common.primitives.Longs;
import com.infomaximum.database.Record;
import com.infomaximum.database.RecordSource;
import com.infomaximum.database.domainobject.filter.Filter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class StoreFileDataTest extends DomainDataTest {

    protected Schema schema;
    protected RecordSource recordSource;

    @BeforeEach
    public void init() throws Exception {
        super.init();
        schema = Schema.read(rocksDBProvider);
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);
        recordSource = new RecordSource(rocksDBProvider);
    }

    protected void testFind(DataEnumerable enumerable, Filter filter, long... expectedIds) throws DatabaseException {
        testFind(enumerable, filter, Longs.asList(expectedIds));
    }

    protected void testFind(Filter filter, long... expectedIds) throws DatabaseException {
        testFind(domainObjectSource, filter, expectedIds);
    }

    protected void testFind(Filter filter, List<Long> expectedIds) throws DatabaseException {
        testFind(domainObjectSource, filter, expectedIds);
    }

    protected void testFind(DataEnumerable enumerable, Filter filter, List<Long> expectedIds) throws DatabaseException {
        List<Long> temp = new ArrayList<>(expectedIds);

        List<Long> foundIds = new ArrayList<>(temp.size());
        try (IteratorEntity<StoreFileReadable> iterator = enumerable.find(StoreFileReadable.class, filter)) {
            while (iterator.hasNext()) {
                foundIds.add(iterator.next().getId());
            }
        }

        temp.sort(Long::compareTo);
        foundIds.sort(Long::compareTo);

        Assertions.assertThat(temp).isEqualTo(foundIds);
    }

    protected  <T extends DomainObject> void assertContainsExactlyDomainObjects(Collection<Record> records, Collection<T> domainObjects) {
        Assertions.assertThat(records).hasSameSizeAs(domainObjects);
        for (Record record : records) {
            T domainObject = domainObjects.stream().filter(t -> t.getId() == record.getId()).findAny().orElseThrow(() -> new NoSuchElementException(record.toString()));
            for (int i = 0; i < record.getValues().length; i++) {
                Assertions.assertThat(record.getValues()[i]).isEqualTo(domainObject.get(i));
            }
        }
    }

    protected <T extends DomainObject> void assertContainsExactlyDomainObjects(Collection<Record> records, T domainObject) {
        Assertions.assertThat(records).hasSize(1);
        for (Record record : records) {
            for (int i = 0; i < record.getValues().length; i++) {
                Assertions.assertThat(record.getValues()[i]).isEqualTo(domainObject.get(i));
            }
        }
    }
}
