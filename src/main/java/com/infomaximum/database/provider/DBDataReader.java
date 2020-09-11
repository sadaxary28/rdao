package com.infomaximum.database.provider;

import com.infomaximum.database.exception.DatabaseException;

public interface DBDataReader {

    DBIterator createIterator(String columnFamily) throws DatabaseException;
    byte[] getValue(String columnFamily, byte[] key) throws DatabaseException;
}
