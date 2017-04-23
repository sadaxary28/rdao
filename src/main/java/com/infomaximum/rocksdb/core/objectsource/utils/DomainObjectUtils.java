package com.infomaximum.rocksdb.core.objectsource.utils;

/**
 * Created by user on 23.04.2017.
 */
public class DomainObjectUtils {

    public static String getRocksDBKey(long id, String fieldName){
        return new StringBuilder().append(id).append('.').append(fieldName).toString();
    }

    public static Object[] parseRocksDBKey(String key) {
        String[] keySplit = key.split("\\.");
        return new Object[]{ Long.parseLong(keySplit[0]), keySplit[1]};
    }
}
