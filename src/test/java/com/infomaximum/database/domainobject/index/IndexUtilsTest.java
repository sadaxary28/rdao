package com.infomaximum.database.domainobject.index;

import com.infomaximum.database.utils.IndexUtils;
import org.junit.Assert;
import org.junit.Test;

public class IndexUtilsTest {

    @Test
    public void buildHashOfString() {
        Assert.assertEquals(0L, IndexUtils.buildHash(String.class, null, null));

        Assert.assertEquals(0L, IndexUtils.buildHash(String.class, "", null));

        Assert.assertEquals(2171775918L, IndexUtils.buildHash(String.class, "english text", null));
        Assert.assertEquals(2171775918L, IndexUtils.buildHash(String.class, "english TEXT", null));

        Assert.assertEquals(4148994842L, IndexUtils.buildHash(String.class, "русский текст", null));
        Assert.assertEquals(4148994842L, IndexUtils.buildHash(String.class, "русский ТЕКСТ", null));

        Assert.assertEquals(480766946L, IndexUtils.buildHash(String.class, "mixed текст", null));

        Assert.assertEquals(2958377010L, IndexUtils.buildHash(String.class, "mixed текс", null));

        Assert.assertEquals(2549465030L, IndexUtils.buildHash(String.class, "mixeed текс", null));
    }
}
