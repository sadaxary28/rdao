package com.infomaximum.rocksdb.exception;

/**
 * Created by user on 23.04.2017.
 */
public class TimeoutLockException extends RuntimeException {

    public TimeoutLockException() {
    }

    public TimeoutLockException(String message) {
        super(message);
    }
}
