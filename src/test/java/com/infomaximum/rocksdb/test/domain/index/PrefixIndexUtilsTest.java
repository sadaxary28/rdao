package com.infomaximum.rocksdb.test.domain.index;

import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import org.junit.Assert;
import org.junit.Test;

import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PrefixIndexUtilsTest {

    @Test
    public void splitIndexedWhitespaceBeginingText() {
        final String text = " Привет Медвед infom.COM  com  test...2d sop@ru \n \r";

        Set<String> lexemes = PrefixIndexUtils.splitIndexingTextIntoLexemes(text);
        Assert.assertTrue(lexemes.remove("привет"));
        Assert.assertTrue(lexemes.remove("медвед"));
        Assert.assertTrue(lexemes.remove("infom.com"));
        Assert.assertTrue(lexemes.remove("com"));
        Assert.assertTrue(lexemes.remove("test...2d"));
        Assert.assertTrue(lexemes.remove("2d"));
        Assert.assertTrue(lexemes.remove("sop@ru"));
        Assert.assertTrue(lexemes.remove("ru"));
        Assert.assertEquals(0, lexemes.size());
    }

    @Test
    public void splitIndexedText() {
        final String text = "Привет Медвед infom.COM  com  ...test...2d sop@ru";

        Set<String> lexemes = PrefixIndexUtils.splitIndexingTextIntoLexemes(text);
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
        final String text = " Привет Медвед infom.COM COM  com  test...2d sop@ru \n \r";

        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед"), text));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив прив"), text));
        Assert.assertFalse(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords("прив мед мед мед мед мед мед мед мед мед мед"), text));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" прив \n мед "), text));
        Assert.assertTrue(PrefixIndexUtils.contains(PrefixIndexUtils.splitSearchingTextIntoWords(" tes co com sop@ "), text));
    }
}
