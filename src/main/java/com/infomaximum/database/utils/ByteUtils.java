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

    public static boolean endsWith(final byte[] suffix, final byte[] source) {
        if (suffix.length > source.length) {
            return false;
        }

        int j = source.length - suffix.length;
        for (int i = 0; i < suffix.length; ++i, ++j) {
            if (source[j] != suffix[i]) {
                return false;
            }
        }
        return true;
    }
}
