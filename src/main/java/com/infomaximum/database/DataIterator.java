package com.infomaximum.database;

import com.infomaximum.database.exception.runtime.DatabaseRuntimeException;

import java.util.Iterator;

public interface DataIterator<T> extends AutoCloseable, Iterator<T> {

//    void reuseReturningRecord(boolean value);

    @Override
    boolean hasNext() throws DatabaseRuntimeException;

    @Override
    T next() throws DatabaseRuntimeException;

    @Override
    void close() throws DatabaseRuntimeException;
}
