package com.infomaximum.database.utils;

import com.infomaximum.database.exception.runtime.IllegalTypeException;

import java.time.Instant;

public class IntervalIndexUtils {

    public static long castToLong(long value) {
        return value;
    }

    public static long castToLong(double value) {
        long val = Double.doubleToRawLongBits(value);
        return val < 0 ? 0x8000000000000000L - val : val;
    }

    public static long castToLong(Instant value) {
        return InstantUtils.toLong(value);
    }

    public static long castToLong(Object value) {
        if (value == null) {
            return 0;
        }

        if (value.getClass() == Long.class) {
            return castToLong(((Long) value).longValue());
        } else if (value.getClass() == Instant.class) {
            return castToLong((Instant) value);
        } else if (value.getClass() == Double.class) {
            return castToLong(((Double) value).doubleValue());
        }

        throw new IllegalTypeException("Unsupported type " + value.getClass());
    }

    public static <T> void checkType(Class<T> indexedClass) {
        if (indexedClass == Long.class ||
                indexedClass == Instant.class ||
                indexedClass == Double.class) {
            return;
        }

        throw new IllegalTypeException("Unsupported type " + indexedClass);
    }

    public static void checkInterval(long begin, long end) {
        if (begin > end) {
            throw new IllegalArgumentException("begin = " + begin + " greater than end = " + end);
        }
    }
}
