package com.infomaximum.database.domainobject.index;

import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.Key;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.nio.LongBuffer;
import java.util.*;

public class PrefixIndexUtilsTest {

    @Test
    public void diffIndexedLexemes() {
        final Set<String> deletingLexemes = new HashSet<>();
        final Set<String> insertingLexemes = new HashSet<>();
        final List<Integer> fields = Arrays.asList(1, 2, 3);
        Map<Integer, Serializable> prevValues = new HashMap<>();
        Map<Integer, Serializable> newValues = new HashMap<>();

        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(Collections.emptySet(), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(Collections.emptySet(), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        newValues.put(1, "test01 test02 test03");
        newValues.put(2, "test12 test12 test13");
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(Collections.emptySet(), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        newValues.put(1, "test12 test12 test13");
        newValues.put(2, "test01 test02 test03");
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(Collections.emptySet(), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        newValues.put(1, "test01 test02 test");
        newValues.put(2, "test13 test");
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(new HashSet<>(Arrays.asList("test03", "test12")), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        newValues.put(1, null);
        newValues.put(2, null);
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(new HashSet<>(Arrays.asList("test01", "test02", "test03", "test12", "test13")), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);

        prevValues.put(1, "test01 test02 test03");
        prevValues.put(2, "test12 test12 test13");
        newValues.put(1, "test01 test02 test03");
        newValues.put(2, null);
        PrefixIndexUtils.diffIndexedLexemes(fields, prevValues, newValues, deletingLexemes, insertingLexemes);
        Assert.assertEquals(new HashSet<>(Arrays.asList("test12", "test13")), deletingLexemes);
        Assert.assertEquals(Collections.emptySet(), insertingLexemes);
    }

    @Test
    public void splitIndexedWhitespaceBeginingText() {
        final String text = " Привет comparator Медвед infom.COM  com   cOmpa comPArat test...2d sop@ru \n \r";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assert.assertArrayEquals(
                new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "comparator", "2d"},
                lexemes.toArray()
        );
    }

    @Test
    public void splitIndexedText() {
        final String text = "Привет Медвед infom.COM  i com  ...test...2d sop@ru м м мед медв";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assert.assertArrayEquals(
                new String[] {"привет", "медвед", "test...2d", "sop@ru", "ru", "infom.com", "com", "2d", "...test...2d"},
                lexemes.toArray()
        );
    }

    @Test
    public void appendId() {
        byte[] bytes = TypeConvert.pack(100L);
        bytes = PrefixIndexUtils.appendId(200, bytes);
        bytes = PrefixIndexUtils.appendId(20, bytes);
        bytes = PrefixIndexUtils.appendId(30, bytes);
        bytes = PrefixIndexUtils.appendId(400, bytes);

        assertArrayEquals(new long[] {20, 30, 100, 200, 400}, bytes);
    }

    @Test
    public void removeId() {
        byte[] bytes = TypeConvert.pack(100L);
        bytes = PrefixIndexUtils.appendId(200, bytes);
        bytes = PrefixIndexUtils.appendId(30, bytes);
        bytes = PrefixIndexUtils.appendId(400, bytes);

        byte[] newBytes = PrefixIndexUtils.removeId(10, bytes);
        Assert.assertNull(newBytes);

        newBytes = PrefixIndexUtils.removeId(200, bytes);
        assertArrayEquals(new long[] {30, 100, 400 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(400, newBytes);
        assertArrayEquals(new long[] {30, 100 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(100, newBytes);
        assertArrayEquals(new long[] { 30 }, newBytes);

        newBytes = PrefixIndexUtils.removeId(30, newBytes);
        Assert.assertArrayEquals(new byte[0], newBytes);
    }

    @Test
    public void getIdCount() {
        byte[] bytes = TypeConvert.allocateBuffer(4 * Key.ID_BYTE_SIZE)
                .putLong(100)
                .putLong(200)
                .putLong(30)
                .putLong(400)
                .array();

        Assert.assertEquals(4, PrefixIndexUtils.getIdCount(bytes));
    }

    @Test
    public void splitSearchingTextIntoWords() {
        final String text = " Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r";

        List<String> lexemes = PrefixIndexUtils.splitSearchingTextIntoWords(text);
        Assert.assertArrayEquals(Arrays.asList(
                "com",
                "com",
                "привет",
                "медвед",
                "sop@ru",
                "infom.com",
                "test...2d").toArray(), lexemes.toArray());
    }

    @Test
    public void contains() {
        final String[] texts = {" Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r", "ru.yandex perl"};
        List<String> tempList = new ArrayList<>();

        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед"), texts, tempList));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив прив"), texts, tempList));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед мед мед мед мед мед мед мед мед мед"), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" прив \n мед "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" tes co com sop@ "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" ru "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru "), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru 2d"), texts, tempList));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" com ru yand"), texts, tempList));
    }

    private static void assertArrayEquals(long[] expected, byte[] actual) {
        LongBuffer buffer = TypeConvert.wrapBuffer(actual).asLongBuffer();
        Assert.assertEquals("Size", expected.length, buffer.limit());
        for (long val : expected) {
            Assert.assertEquals(val, buffer.get());
        }
    }
}
