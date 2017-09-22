package com.infomaximum.database.core.sequence;

import org.rocksdb.RocksDBException;

public class SequenceDBException extends RocksDBException {

    public SequenceDBException() {
        super("Sequence already exists.");
    }
}
