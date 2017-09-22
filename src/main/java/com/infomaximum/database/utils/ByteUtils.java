package com.infomaximum.database.utils;

public class ByteUtils {

    public static boolean startsWith(final byte[] prefix, final byte[] source) {
        return startsWith(prefix, 0, source);
    }

    public static boolean startsWith(final byte[] prefix, int offset, final byte[] source) {
        if (prefix.length > (source.length - offset)) {
            return false;
        }

        for (int i = 0; i < prefix.length; ++i, ++offset) {
            if (source[offset] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
