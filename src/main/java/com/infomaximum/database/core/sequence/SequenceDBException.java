package com.infomaximum.database.core.sequence;

import org.rocksdb.RocksDBException;

public class SequenceDBException extends RocksDBException {

    public SequenceDBException(String name) {
        super("Sequence " + name + " already exists.");
    }
}
