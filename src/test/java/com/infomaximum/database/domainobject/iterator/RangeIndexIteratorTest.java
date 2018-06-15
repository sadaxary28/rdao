package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class RangeIndexIteratorTest extends StoreFileDataTest {

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

        assertEquals("  |___|", Collections.emptyList());

        test("    |___|", "",
                "|V____|"
        );
        test(" |___|","",
                "   |V____|"
        );
        test("    |____|","",
                "   |V____|",
                "|__|"
        );
        test("    |____|","",
                "   |V______|",
                "           |__|"
        );
        test("      |_____|","",
                "       |V____|",
                "        |V____|",
                "|___|",
                "|___|"
        );
        test("    |____|","",
                "        |V____|",
                "|V____|"
        );
        test("       |_____|","",
                "       |V____|",
                "       |V____|",
                "|____|"
        );
        test("|___|", "",
                "    |_____|"
        );
        test("      |___|","",
                "|_____|"
        );
        test("       |___|","",
                "|____|"
        );
        test(" |___|","",
                "       |____|"
        );
        test(" |___|","",
                "|V________|",
                "   |V__|"
        );
        test(" |___|","",
                "   |V__|",
                "|V________|"
        );
        test(" |____________|","",
                "   |V__|",
                "|V________|",
                "      |V_____|",
                "                |_____|"
        );
        test(" |____________|", "",
                "   |V__|",
                "|V________|",
                "   |V_____|",
                "                |_____|"
        );
        test("      |____|","",
                "      |V__|",
                "    |V________|",
                "      |V_____|",
                "|_|",
                " |_|",
                " |V_______________|"
        );
    }

    @Test
    public void groupedInsertAndFind() throws Exception {
        test(" |___________2|", "",
                "   |__1|",
                "|________1|"
        );
        test(" |___________1|","",
                "   |V__1|",
                "|________2|"
        );
        test(" |___________1|","",
                "   |V__1|",
                "|________2|",
                "       |V________1|"
        );
        test("      |____1|","",
                "   |V__1|",
                "|________2|",
                "     |V________1|",
                " |__1|"
        );
    }

    /**
     * @param f filter
     */
    private void test(String f, String... insertingIntervals) throws Exception {
        List<StoreFileReadable> expected = new ArrayList<>(insertingIntervals.length);
        Assert.assertEquals("", insertingIntervals[0]);

        domainObjectSource.executeTransactional(transaction -> {
            transaction.setForeignFieldEnabled(false);

            for (int i = 1; i < insertingIntervals.length; ++i) {

                Interval interval = parseInterval(insertingIntervals[i]);

                StoreFileEditable s = transaction.create(StoreFileEditable.class);
                s.setBegin(interval.begin);
                s.setEnd(interval.end);
                s.setFolderId(interval.folderId);
                transaction.save(s);

                if (interval.isMatched) {
                    expected.add(s);
                }
            }
        });

        assertEquals(f, expected);

        destroy();
        init();
    }

    private void assertEquals(String filter, List<StoreFileReadable> expected) throws DatabaseException {
        List<StoreFileReadable> actual = new ArrayList<>(expected.size());
        Interval interval = parseInterval(filter);

        RangeFilter rangeFilter = new RangeFilter(StoreFileReadable.RANGE_INDEXED_FIELD, interval.begin, interval.end);
        if (interval.folderId != null) {
            rangeFilter.appendHashedField(StoreFileReadable.FIELD_FOLDER_ID, interval.folderId);
        }
        try (IteratorEntity<StoreFileReadable> i = domainObjectSource.find(StoreFileReadable.class, rangeFilter)) {
            while (i.hasNext()) {
                actual.add(i.next());
            }
        }

        expected.sort(Comparator.comparingLong(StoreFileReadable::getBegin));
        Assert.assertEquals(expected, actual);
    }

    private static Interval parseInterval(String src) {
        int begin = src.indexOf('|');
        int end = src.indexOf('|', begin + 1);
        Long folderId = Character.isDigit(src.charAt(end - 1)) ? (long) Character.getNumericValue(src.charAt(end - 1)) : null;

        return new Interval(begin, end, src.charAt(begin + 1) == 'V', folderId);
    }

    private static class Interval {

        final long begin;
        final long end;
        final boolean isMatched;
        final Long folderId;

        Interval(long begin, long end, boolean isMatched, Long folderId) {
            this.begin = begin;
            this.end = end;
            this.isMatched = isMatched;
            this.folderId = folderId;
        }
    }
}
