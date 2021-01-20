package com.infomaximum.database.domainobject;

import com.google.common.primitives.Longs;
import com.infomaximum.database.DataCommand;
import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.RecordSource;
import com.infomaximum.database.domainobject.filter.Filter;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.filter.IdFilter;
import com.infomaximum.database.domainobject.filter.PrefixFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.domain.StoreFileReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public abstract class StoreFileDataTest extends DomainDataTest {

    protected static final String STORE_FILE_NAMESPACE = "com.infomaximum.store";
    protected static final String STORE_FILE_NAME = "StoreFile";
    protected static final String FOLDER_FILE_NAME = "ExchangeFolder";
    protected static final String FOLDER_FILE_NAMESPACE = "com.infomaximum.exchange";

    protected Schema schema;
    protected RecordSource recordSource;

    @BeforeEach
    public void init() throws Exception {
        super.init();
        schema = Schema.read(rocksDBProvider);
        createDomain(ExchangeFolderReadable.class);
        createDomain(StoreFileReadable.class);
        recordSource = new RecordSource(rocksDBProvider);
        schema = Schema.read(rocksDBProvider);
    }

    @Deprecated
    protected void assertFind(DataEnumerable enumerable, Filter filter, long... expectedIds) throws Exception {
        assertFind(filter, expectedIds);
    }

    protected void assertFind(DataCommand dataCommand, Filter filter, long... expectedIds) throws DatabaseException {
        assertFind(dataCommand, filter, Longs.asList(expectedIds));
    }

    protected void assertFind(Filter filter, List<Long> expectedIds) throws Exception {
        recordSource.executeTransactional(dataCommand -> {assertFind(dataCommand, filter, expectedIds);});
    }

    protected void assertFind(Filter filter, long... expectedIds) throws Exception {
        assertFind(filter, LongStream.of(expectedIds).boxed().collect(Collectors.toList()));
    }

    protected void assertFind(DataCommand dataCommand, Filter filter, List<Long> expectedIds) throws DatabaseException {
        List<Long> temp = new ArrayList<>(expectedIds);

        List<Long> foundIds = new ArrayList<>(temp.size());
        try (RecordIterator iterator = getIteratorForFilter(STORE_FILE_NAME, STORE_FILE_NAMESPACE, filter, dataCommand)) {
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

    private RecordIterator getIteratorForFilter(String tableName, String namespace, Filter filter, DataCommand dataCommand) {
        if (filter instanceof HashFilter) {
            return dataCommand.select(tableName, namespace, (HashFilter) filter);
        }
        if (filter instanceof PrefixFilter) {
            return dataCommand.select(tableName, namespace, (PrefixFilter) filter);
        }
        if (filter instanceof IdFilter) {
            return dataCommand.select(tableName, namespace, (IdFilter) filter);
        }
        throw new UnsupportedOperationException();
    }
}
