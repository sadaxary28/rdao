package com.infomaximum.database.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class LocalDateTimeUtils {

    private static final ZoneOffset OFFSET = ZoneOffset.UTC;

    public static long toLong(LocalDateTime value) {
        return InstantUtils.toLong(value.toInstant(OFFSET));
    }

    public static LocalDateTime fromLong(long value) {
        Instant instant = InstantUtils.fromLong(value);
        return LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), OFFSET);
    }
}
