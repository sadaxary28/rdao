package com.infomaximum.database.utils.key;

abstract class IndexKey extends Key{

    static final int FIELDS_HASH_BYTE_SIZE = 4;

    final byte[] fieldsHash;

    IndexKey(long id, byte[] fieldsHash) {
        super(id);
        this.fieldsHash = fieldsHash;
    }
}