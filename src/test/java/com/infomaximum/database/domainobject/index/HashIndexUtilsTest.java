package com.infomaximum.database.domainobject.index;

import com.infomaximum.database.utils.HashIndexUtils;
import org.junit.Assert;
import org.junit.Test;

public class HashIndexUtilsTest {

    @Test
    public void buildHashOfString() {
        Assert.assertEquals(0L, HashIndexUtils.buildHash(String.class, null, null));

        Assert.assertEquals(0L, HashIndexUtils.buildHash(String.class, "", null));

        Assert.assertEquals(2171775918L, HashIndexUtils.buildHash(String.class, "english text", null));
        Assert.assertEquals(2171775918L, HashIndexUtils.buildHash(String.class, "english TEXT", null));

        Assert.assertEquals(4148994842L, HashIndexUtils.buildHash(String.class, "русский текст", null));
        Assert.assertEquals(4148994842L, HashIndexUtils.buildHash(String.class, "русский ТЕКСТ", null));

        Assert.assertEquals(480766946L, HashIndexUtils.buildHash(String.class, "mixed текст", null));

        Assert.assertEquals(2958377010L, HashIndexUtils.buildHash(String.class, "mixed текс", null));

        Assert.assertEquals(2549465030L, HashIndexUtils.buildHash(String.class, "mixeed текс", null));
    }
}
