package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import org.junit.Assert;
import org.junit.Test;

import java.nio.LongBuffer;
import java.util.*;

public class PrefixIndexUtilsTest {

    @Test
    public void diffIndexedLexemes() {
        final Set<String> deletingLexemes = new HashSet<>();
        final Set<String> insertingLexemes = new HashSet<>();
        final List<Integer> fields = Arrays.asList(1, 2, 3);
        Map<Integer, Object> prevValues = new HashMap<>();
        Map<Integer, Object> newValues = new HashMap<>();

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
        Assert.assertTrue(lexemes.remove("привет"));
        Assert.assertTrue(lexemes.remove("медвед"));
        Assert.assertTrue(lexemes.remove("infom.com"));
        Assert.assertTrue(lexemes.remove("comparator"));
        Assert.assertTrue(lexemes.remove("test...2d"));
        Assert.assertTrue(lexemes.remove("2d"));
        Assert.assertTrue(lexemes.remove("sop@ru"));
        Assert.assertTrue(lexemes.remove("ru"));
        Assert.assertEquals(0, lexemes.size());
    }

    @Test
    public void splitIndexedText() {
        final String text = "Привет Медвед infom.COM  com  ...test...2d sop@ru";

        SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();
        PrefixIndexUtils.splitIndexingTextIntoLexemes(text, lexemes);
        Assert.assertTrue(lexemes.remove("привет"));
        Assert.assertTrue(lexemes.remove("медвед"));
        Assert.assertTrue(lexemes.remove("infom.com"));
        Assert.assertTrue(lexemes.remove("com"));
        Assert.assertTrue(lexemes.remove("...test...2d"));
        Assert.assertTrue(lexemes.remove("test...2d"));
        Assert.assertTrue(lexemes.remove("2d"));
        Assert.assertTrue(lexemes.remove("sop@ru"));
        Assert.assertTrue(lexemes.remove("ru"));
        Assert.assertEquals(0, lexemes.size());
    }

    @Test
    public void appendId() {
        byte[] bytes = TypeConvert.allocateBuffer(Key.ID_BYTE_SIZE).putLong(100).array();
        bytes = PrefixIndexUtils.appendId(10, bytes);
        LongBuffer buffer = TypeConvert.wrapBuffer(bytes).asLongBuffer();
        Assert.assertEquals(100, buffer.get());
        Assert.assertEquals(10, buffer.get());
        Assert.assertEquals(2, buffer.position());
    }

    @Test
    public void removeId() {
        byte[] bytes = TypeConvert.allocateBuffer(4 * Key.ID_BYTE_SIZE)
                .putLong(100)
                .putLong(200)
                .putLong(30)
                .putLong(400)
                .array();

        byte[] newBytes = PrefixIndexUtils.removeId(10, bytes);
        Assert.assertNull(newBytes);

        newBytes = PrefixIndexUtils.removeId(200, bytes);
        LongBuffer buffer = TypeConvert.wrapBuffer(newBytes).asLongBuffer();
        Assert.assertEquals(100, buffer.get());
        Assert.assertEquals(30, buffer.get());
        Assert.assertEquals(400, buffer.get());
        Assert.assertEquals(3, buffer.position());

        newBytes = PrefixIndexUtils.removeId(400, newBytes);
        buffer = TypeConvert.wrapBuffer(newBytes).asLongBuffer();
        Assert.assertEquals(100, buffer.get());
        Assert.assertEquals(30, buffer.get());
        Assert.assertEquals(2, buffer.position());

        newBytes = PrefixIndexUtils.removeId(100, newBytes);
        buffer = TypeConvert.wrapBuffer(newBytes).asLongBuffer();
        Assert.assertEquals(30, buffer.get());
        Assert.assertEquals(1, buffer.position());

        newBytes = PrefixIndexUtils.removeId(30, newBytes);
        Assert.assertEquals(0, newBytes.length);
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
}
