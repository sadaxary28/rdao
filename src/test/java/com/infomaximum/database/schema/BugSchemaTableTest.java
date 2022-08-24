package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.domainobject.filter.PrefixFilter;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.domainobject.iterator.HashIndexIterator;
import com.infomaximum.database.domainobject.iterator.IntervalIndexIterator;
import com.infomaximum.database.domainobject.iterator.PrefixIndexIterator;
import com.infomaximum.database.domainobject.iterator.RangeIndexIterator;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBTable;
import com.infomaximum.database.schema.dbstruct.DBTableTestUtil;
import com.infomaximum.database.schema.table.*;
import com.infomaximum.domain.IndexRecreationEditable;
import com.infomaximum.domain.IndexRecreationReadable;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BugSchemaTableTest extends DomainDataJ5Test {

    private static DBTable getExpectedDbTable() {
        DBField fieldGEnd = DBTableTestUtil.buildDBField(0, "g_end", Instant.class, null);
        DBField fieldAPrice = DBTableTestUtil.buildDBField(1, "a_price", Long.class, null);
        DBField fieldXName = DBTableTestUtil.buildDBField(2, "x_name", String.class, null);
        DBField fieldAAmount = DBTableTestUtil.buildDBField(3, "a_amount", Long.class, null);
        DBField fieldAType = DBTableTestUtil.buildDBField(4, "a_type", Boolean.class, null);
        DBField fieldSBegin = DBTableTestUtil.buildDBField(5, "s_begin", Instant.class, null);
        DBField fieldZName = DBTableTestUtil.buildDBField(6, "z_name", String.class, null);

        DBTable expected = DBTableTestUtil.buildDBTable(0, "IndexRecreation", "com.infomaximum.rocksdb", new ArrayList<DBField>() {{
            add(fieldGEnd);
            add(fieldAPrice);
            add(fieldXName);
            add(fieldAAmount);
            add(fieldAType);
            add(fieldSBegin);
            add(fieldZName);
        }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(fieldAPrice, fieldZName));
        expected.attachIndex(DBTableTestUtil.buildDBPrefixIndex(fieldXName, fieldZName));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(fieldAAmount, fieldAPrice, fieldZName));
        expected.attachIndex(DBTableTestUtil.buildDBRangeIndex(fieldSBegin, fieldGEnd, fieldAPrice, fieldZName));
        return expected;
    }

    @Test
    @DisplayName("Создается таблица заполняется данными, после чего удаляем и заново создаем индекс в таблице.")
    void removeAndAttachHashIndexTest() throws Exception {
        final Table indexRecreationTable = createIndexRecreationTable();
        final THashIndex hashIndex = new THashIndex("a_price", "z_name");
        final TPrefixIndex prefixIndex = new TPrefixIndex("x_name", "z_name");
        final TIntervalIndex intervalIndex = new TIntervalIndex("a_amount", new String[]{"z_name", "a_price"});
        final TRangeIndex rangeIndex = new TRangeIndex("s_begin", "g_end", new String[]{"z_name", "a_price"});

        schema.dropIndex(hashIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.dropIndex(prefixIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.dropIndex(intervalIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.dropIndex(rangeIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());

        schema.createIndex(hashIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.createIndex(prefixIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.createIndex(intervalIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());
        schema.createIndex(rangeIndex, indexRecreationTable.getName(), indexRecreationTable.getNamespace());

        DBTableTestUtil.assertThatContains(rocksDBProvider, getExpectedDbTable());

        testHashIndexDirectOrder();
        testHashIndexReverseOrder();

        testPrefixIndexDirectOrder();
        testPrefixIndexReverseOrder();

        testIntervalFilterDirectOrder();
        testIntervalFilterReverseOrder();

        testRangeFilterDirectOrder();
        testRangeFilterReverseOrder();
    }

    private void testRangeFilterReverseOrder() {
        String name = "nameZ2";
        Long price = 3080L;

        final RangeFilter filter = new RangeFilter(
                new RangeFilter.IndexedField(IndexRecreationReadable.FIELD_S_BEGIN, IndexRecreationReadable.FIELD_G_END),
                Instant.EPOCH.plus(2, ChronoUnit.MINUTES),
                Instant.EPOCH.plus(6, ChronoUnit.MINUTES))
                .appendHashedField(IndexRecreationReadable.FIELD_PRICE, price)
                .appendHashedField(IndexRecreationReadable.FIELD_NAME_Z, name);


        try (RangeIndexIterator<IndexRecreationReadable> hashIndexIterator = new RangeIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameZ()).isEqualTo(name);
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getAmount()).isEqualTo(190L);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testRangeFilterDirectOrder() {
        String name = "nameZ2";
        Long price = 3080L;

        final RangeFilter filter = new RangeFilter(
                new RangeFilter.IndexedField(IndexRecreationReadable.FIELD_S_BEGIN, IndexRecreationReadable.FIELD_G_END),
                Instant.EPOCH.plus(2, ChronoUnit.MINUTES),
                Instant.EPOCH.plus(6, ChronoUnit.MINUTES))
                .appendHashedField(IndexRecreationReadable.FIELD_NAME_Z, name)
                .appendHashedField(IndexRecreationReadable.FIELD_PRICE, price);


        try (RangeIndexIterator<IndexRecreationReadable> hashIndexIterator = new RangeIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameZ()).isEqualTo(name);
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getAmount()).isEqualTo(190L);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testIntervalFilterDirectOrder() {
        String name = "nameZ2";
        Long price = 3080L;
        IntervalFilter filter = new IntervalFilter(IndexRecreationReadable.FIELD_AMOUNT, 5L, 200L)
                .appendHashedField(IndexRecreationReadable.FIELD_NAME_Z, name)
                .appendHashedField(IndexRecreationReadable.FIELD_PRICE, price);

        try (IntervalIndexIterator<IndexRecreationReadable> hashIndexIterator = new IntervalIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameZ()).isEqualTo(name);
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getAmount()).isEqualTo(190L);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testIntervalFilterReverseOrder() {
        String name = "nameZ2";
        Long price = 3080L;
        IntervalFilter filter = new IntervalFilter(IndexRecreationReadable.FIELD_AMOUNT, 5L, 200L)
                .appendHashedField(IndexRecreationReadable.FIELD_PRICE, price)
                .appendHashedField(IndexRecreationReadable.FIELD_NAME_Z, name);

        try (IntervalIndexIterator<IndexRecreationReadable> hashIndexIterator = new IntervalIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameZ()).isEqualTo(name);
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getAmount()).isEqualTo(190L);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testPrefixIndexDirectOrder() {

        String name = "nameX3";
        PrefixFilter filter = new PrefixFilter(Arrays.asList(IndexRecreationReadable.FIELD_NAME_X, IndexRecreationReadable.FIELD_NAME_Z), name);


        try (PrefixIndexIterator<IndexRecreationReadable> hashIndexIterator = new PrefixIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameX()).isEqualTo(name);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testPrefixIndexReverseOrder() {

        String name = "nameZ3";
        PrefixFilter filter = new PrefixFilter(Arrays.asList(IndexRecreationReadable.FIELD_NAME_Z, IndexRecreationReadable.FIELD_NAME_X), name);


        try (PrefixIndexIterator<IndexRecreationReadable> hashIndexIterator = new PrefixIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getNameZ()).isEqualTo(name);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }

    private void testHashIndexDirectOrder() {
        Long price = 3080L;
        String zName = "nameZ2";
        HashFilter filter = new HashFilter(IndexRecreationReadable.FIELD_PRICE, price)
                .appendField(IndexRecreationReadable.FIELD_NAME_Z, zName);


        try (HashIndexIterator<IndexRecreationReadable> hashIndexIterator = new HashIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getNameZ()).isEqualTo(zName);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }


    private void testHashIndexReverseOrder() {
        Long price = 3080L;
        String zName = "nameZ2";
        HashFilter filter = new HashFilter(IndexRecreationReadable.FIELD_NAME_Z, zName)
                .appendField(IndexRecreationReadable.FIELD_PRICE, price);


        try (HashIndexIterator<IndexRecreationReadable> hashIndexIterator = new HashIndexIterator<>(new DomainObjectSource(rocksDBProvider),
                IndexRecreationReadable.class,
                null,
                filter
        )) {
            int iteratedRecordCount = 0;
            while (hashIndexIterator.hasNext()) {
                final IndexRecreationReadable readable = hashIndexIterator.next();
                Assertions.assertThat(readable.getPrice()).isEqualTo(price);
                Assertions.assertThat(readable.getNameZ()).isEqualTo(zName);
                ++iteratedRecordCount;
            }
            Assertions.assertThat(iteratedRecordCount).isEqualTo(1);
        }
    }


    private Table createIndexRecreationTable() throws Exception {
        Schema.resolve(IndexRecreationReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("g_end", Instant.class));
            add(new TField("a_price", Long.class));
            add(new TField("x_name", String.class));
            add(new TField("a_amount", Long.class));
            add(new TField("a_type", Boolean.class));
            add(new TField("s_begin", Instant.class));
            add(new TField("z_name", String.class));
        }};

        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("z_name", "a_price"));
        }};

        List<TPrefixIndex> prefixIndexes = new ArrayList<TPrefixIndex>() {{
            add(new TPrefixIndex("x_name", "z_name"));
        }};

        List<TIntervalIndex> intervalIndexes = new ArrayList<TIntervalIndex>() {{
            add(new TIntervalIndex("a_amount", new String[]{"z_name", "a_price"}));
        }};

        List<TRangeIndex> rangeIndexes = new ArrayList<TRangeIndex>() {{
            add(new TRangeIndex("s_begin", "g_end", new String[]{"z_name", "a_price"}));
        }};

        Table table = new Table("IndexRecreation",
                "com.infomaximum.rocksdb",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes);
        schema.createTable(table);


        domainObjectSource.executeTransactional(transaction -> {
            IndexRecreationEditable editable = transaction.create(IndexRecreationEditable.class);
            editable.setNameZ("nameZ1");
            editable.setType(true);
            editable.setBegin(Instant.EPOCH.plus(1, ChronoUnit.MINUTES));
            editable.setNameX("nameX1");
            editable.setAmount(10L);
            editable.setPrice(300L);
            editable.setEnd(Instant.EPOCH.plus(2, ChronoUnit.MINUTES));
            transaction.save(editable);

            editable = transaction.create(IndexRecreationEditable.class);
            editable.setNameZ("nameZ2");
            editable.setType(false);
            editable.setBegin(Instant.EPOCH.plus(3, ChronoUnit.MINUTES));
            editable.setNameX("nameX2");
            editable.setAmount(190L);
            editable.setPrice(3080L);
            editable.setEnd(Instant.EPOCH.plus(4, ChronoUnit.MINUTES));
            transaction.save(editable);

            editable = transaction.create(IndexRecreationEditable.class);
            editable.setNameZ("nameZ3");
            editable.setType(Boolean.TRUE);
            editable.setBegin(Instant.EPOCH.plus(5, ChronoUnit.MINUTES));
            editable.setNameX("nameX3");
            editable.setAmount(1920L);
            editable.setPrice(30830L);
            editable.setEnd(Instant.EPOCH.plus(6, ChronoUnit.MINUTES));
            transaction.save(editable);
        });

        return schema.getTable("IndexRecreation", "com.infomaximum.rocksdb");
    }
}