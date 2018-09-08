package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.utils.InstantUtils;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class RangeIndexIteratorTest extends StoreFileDataTest {

    @Test
    public void insertAndFindZeroLengthRange() throws Exception {
        final Long value = 10L;
        final Long folderId = 20L;

        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBegin(value);
            s.setEnd(value);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(value - 1);
            s.setEnd(value + 1);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setFolderId(folderId);
            s.setBegin(value);
            s.setEnd(value);
            transaction.save(s);
        });

        assertEquals(Collections.singletonList(3L), findAll(new Interval(value, value, true, folderId)));
        assertEquals(Arrays.asList(1L, 3L), findAll(new Interval(value, value)));
        assertEquals(Arrays.asList(1L, 2L, 3L), findAll(new Interval(value, value + 1)));
        assertEquals(Collections.singletonList(2L), findAll(new Interval(value - 1, value)));
        assertEquals(Arrays.asList(1L, 2L, 3L), findAll(new Interval(value - 1, value + 1)));

        domainObjectSource.executeTransactional(transaction -> {
            try (IteratorEntity<StoreFileEditable> i = domainObjectSource.find(StoreFileEditable.class, EmptyFilter.INSTANCE)) {
                while (i.hasNext()) {
                    transaction.remove(i.next());
                }
            }
        });

        new DomainService(rocksDBProvider)
                .setChangeMode(ChangeMode.NONE)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(StoreFileEditable.class))
                .execute();
    }

    private void assertEquals(List<Long> expectedIds, List<StoreFileReadable> actual) {
        Assert.assertEquals(expectedIds, actual.stream().map(DomainObject::getId).sorted().collect(Collectors.toList()));
    }

    @Test
    public void insertAndFindLargeInstant() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBeginTime(InstantUtils.MIN);
            s.setEndTime(InstantUtils.ZERO);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBeginTime(InstantUtils.ZERO.plus(Duration.ofSeconds(1)));
            s.setEndTime(InstantUtils.MAX);
            transaction.save(s);
        });

        RangeFilter rangeFilter = new RangeFilter(StoreFileReadable.RANGE_INSTANT_FIELD, InstantUtils.MIN, InstantUtils.ZERO.minus(Duration.ofMillis(1)));
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, rangeFilter)) {
            Assert.assertTrue(i.hasNext());
        }

        rangeFilter = new RangeFilter(StoreFileReadable.RANGE_INSTANT_FIELD, InstantUtils.ZERO.plus(Duration.ofMillis(1)), InstantUtils.MAX);
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, rangeFilter)) {
            Assert.assertTrue(i.hasNext());
        }

        try {
            rangeFilter = new RangeFilter(StoreFileReadable.RANGE_INSTANT_FIELD, InstantUtils.ZERO.plus(Duration.ofMillis(1)), InstantUtils.MAX.plus(Duration.ofMillis(1)));
            try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, rangeFilter)) {
                Assert.assertTrue(i.hasNext());
            }
            Assert.fail();
        } catch (ArithmeticException ignore) {
        }
    }

    @Test
    public void insertAndFind() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBegin(1L);
            s.setEnd(null);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(null);
            s.setEnd(1L);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(null);
            s.setEnd(null);
            transaction.save(s);
        });

        assertEquals(Collections.emptyList(), findAll(new Interval(0, 5)));

        test(0,
                "    |___|", "",
                   "|V____|");
        test(100,
                " |___|","",
                   "   |V____|"
        );
        test(200,
                "    |____|","",
                   "   |V____|",
                   "|__|"
        );
        test(300,
                "    |____|","",
                   "   |V______|",
                   "           |__|"
        );
        test(400,
                "      |_____|","",
                   "       |V____|",
                   "        |V____|",
                   "|___|",
                   "|___|"
        );
        test(500,
                "    |____|","",
                   "        |V____|",
                   "|V____|"
        );
        test(600,
                "       |_____|","",
                   "       |V____|",
                   "       |V____|",
                   "|____|"
        );
        test(700,
                "|___|", "",
                   "    |_____|"
        );
        test(800,
                "      |___|","",
                   "|_____|"
        );
        test(900,
                "       |___|","",
                   "|____|"
        );
        test(1000,
                " |___|","",
                   "       |____|"
        );
        test(1100,
                " |___|","",
                   "|V________|",
                   "   |V__|"
        );
        test(1200,
                " |___|","",
                   "   |V__|",
                   "|V________|"
        );
        test(1300,
                " |____________|","",
                   "   |V__|",
                   "|V________|",
                   "      |V_____|",
                   "                |_____|"
        );
        test(1400,
                " |____________|", "",
                   "   |V__|",
                   "|V________|",
                   "   |V_____|",
                   "                |_____|"
        );
        test(1500,
                "      |____|","",
                   "      |V__|",
                   "    |V________|",
                   "      |V_____|",
                   "|_|",
                   " |_|",
                   " |V_______________|"
        );

        test(1600,
                "            |_|","",
                   "        |V_____|",
                   "         |_|"
        );
        test(1700,
                "            |_|","",
                   "        |V_____|",
                   "        |V_____|",
                   "        |V_____|",
                   "         |_|"
        );
        test(1800,
                "           |_|","",
                  "        |V____|",
                  "        |_|"
        );
        test(1900,
                "          |_|","",
                   "        |V____|",
                   "        |_|"
        );
        test(2000,
                "           |__|","",
                   "        |V____|",
                   "        |_|"
        );
        test(2100,
                "           |_|","",
                   "        |V____|",
                   "        |V__|",
                   "        |_| "
        );
    }

    @Test
    public void groupedInsertAndFind() throws Exception {
        test(0,
                " |___________2|", "",
                   "   |__1|",
                   "|________1|"
        );
        test(100,
                " |___________1|","",
                   "   |V__1|",
                   "|________2|"
        );
        test(200,
                " |___________1|","",
                   "   |V__1|",
                   "|________2|",
                   "       |V________1|"
        );
        test(300,
                "      |____1|","",
                   "   |V__1|",
                   "|________2|",
                   "     |V________1|",
                   " |__1|"
        );
        test(400,
                "            |1|","",
                   "        |V____1|",
                   "         |1|"
        );
        test(500,
                "           |1|","",
                   "        |V___1|",
                   "        |V___1|",
                   "        |____2|",
                   "        |1|"
        );
        test(600,
                "          |1|","",
                   "        |V___1|",
                   "        |1|"
        );
        test(700,
                "           |-1|","",
                   "        |V___1|",
                   "        |1|"
        );

        List<Long> expectedIds = new ArrayList<>();
        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBeginTime(toInstant(9, 1, 2018));
            s.setEndTime(toInstant(14, 1, 2018));
            s.setFolderId(1);
            transaction.save(s);
            expectedIds.add(s.getId());

            s = transaction.create(StoreFileEditable.class);
            s.setBeginTime(toInstant(9, 1, 2018));
            s.setEndTime(toInstant(11, 1, 2018));
            s.setFolderId(1);
            transaction.save(s);
        });

        RangeFilter filter = new RangeFilter(
                StoreFileReadable.RANGE_INSTANT_FIELD,
                toInstant(12, 1, 2018),
                toInstant(13, 1, 2018)
        ).appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, 1L);

        List<Long> ids = new ArrayList<>();
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, filter)) {
            while (i.hasNext()) {
                ids.add(i.next().getId());
            }
        }
        Assert.assertEquals(expectedIds, ids);
    }

    private static Instant toInstant(int day, int month, int year) {
        return ZonedDateTime.of(
                year, month, day, 0, 0, 0, 0, ZoneId.of("Europe/Moscow")).toInstant();
    }

    @Test
    public void updateAndFind() throws Exception {
        final long id1 = 1L;
        final long id2 = 2L;
        final List<StoreFileReadable> expected = Arrays.asList(
                new StoreFileReadable(id1), new StoreFileReadable(id2)
        );

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBegin(10L);
            s.setEnd(20L);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(20L);
            s.setEnd(21L);
            transaction.save(s);
        });

        Assert.assertEquals(expected, findAll(new Interval(0, 40)));

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable s = transaction.get(StoreFileEditable.class, id2);
            s.setEnd(22L);
            transaction.save(s);
        });

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable s = transaction.get(StoreFileEditable.class, id1);
            s.setEnd(23L);
            transaction.save(s);
        });

        Assert.assertEquals(expected, findAll(new Interval(0, 40)));
    }

    @Test
    public void deleteAndFind() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            StoreFileEditable s = transaction.create(StoreFileEditable.class);
            s.setBegin(10L);
            s.setEnd(20L);
            s.setFolderId(1);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(15L);
            s.setEnd(25L);
            s.setFolderId(2);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(16L);
            s.setEnd(28L);
            s.setFolderId(2);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(15L);
            s.setEnd(18L);
            s.setFolderId(2);
            transaction.save(s);

            s = transaction.create(StoreFileEditable.class);
            s.setBegin(35L);
            s.setEnd(35L);
            s.setFolderId(2);
            transaction.save(s);
        });

        domainObjectSource.executeTransactional(transaction -> {
            try (IteratorEntity<StoreFileEditable> i = domainObjectSource.find(StoreFileEditable.class, EmptyFilter.INSTANCE)) {
                while (i.hasNext()) {
                    transaction.remove(i.next());
                }
            }
        });

        Assert.assertEquals(Collections.emptyList(), findAll(new Interval(0, 50)));
    }

    /**
     * @param f filter
     */
    private void test(int intervalShift, String f, String... insertingIntervals) throws Exception {
        List<StoreFileReadable> expected = new ArrayList<>(insertingIntervals.length);
        Assert.assertEquals("", insertingIntervals[0]);

        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            for (int i = 1; i < insertingIntervals.length; ++i) {

                Interval interval = parseInterval(insertingIntervals[i]);

                StoreFileEditable s = transaction.create(StoreFileEditable.class);
                s.setBegin(interval.begin + intervalShift);
                s.setEnd(interval.end + intervalShift);
                s.setFolderId(interval.folderId);
                transaction.save(s);

                if (interval.isMatched) {
                    expected.add(s);
                }
            }
        });

        Interval interval = parseInterval(f);
        interval.plus(intervalShift);
        List<StoreFileReadable> actual = findAll(interval);

        Comparator<StoreFileReadable> comparator = Comparator.comparingLong(StoreFileReadable::getBegin)
                .thenComparing(Comparator.comparingLong(StoreFileReadable::getId));
        expected.sort(comparator);
        actual.sort(comparator);
        Assert.assertEquals(expected, actual);
    }

    private List<StoreFileReadable> findAll(Interval interval) throws DatabaseException {
        List<StoreFileReadable> result = new ArrayList<>();

        RangeFilter rangeFilter = new RangeFilter(StoreFileReadable.RANGE_LONG_FIELD, interval.begin, interval.end);
        if (interval.folderId != null) {
            rangeFilter.appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, interval.folderId);
        }
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, rangeFilter)) {
            while (i.hasNext()) {
                result.add(i.next());
            }
        }

        return result;
    }

    private static Interval parseInterval(String src) {
        int begin = src.indexOf('|');
        int end = src.indexOf('|', begin + 1);
        Long folderId = Character.isDigit(src.charAt(end - 1)) ? (long) Character.getNumericValue(src.charAt(end - 1)) : null;

        return new Interval(begin, end, src.charAt(begin + 1) == 'V', folderId);
    }

    private static class Interval {

        long begin;
        long end;
        final boolean isMatched;
        final Long folderId;

        Interval(long begin, long end, boolean isMatched, Long folderId) {
            this.begin = begin;
            this.end = end;
            this.isMatched = isMatched;
            this.folderId = folderId;
        }

        Interval(long begin, long end) {
            this(begin, end, false, null);
        }

        void plus(long value) {
            begin += value;
            end += value;
        }
    }
}
