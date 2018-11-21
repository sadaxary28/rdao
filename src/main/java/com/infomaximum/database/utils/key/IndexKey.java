package com.infomaximum.database.utils.key;

abstract class IndexKey extends Key{

    static final int FIELDS_HASH_BYTE_SIZE = 4;

    final byte[] fieldsHash;

    IndexKey(long id, byte[] fieldsHash) {
        super(id);
        checkFieldsHash(fieldsHash);
        this.fieldsHash = fieldsHash;
    }

    static void checkFieldsHash(final byte[] fieldsHash) {
        if(fieldsHash == null || fieldsHash.length == 0 || fieldsHash.length != FIELDS_HASH_BYTE_SIZE) {
            throw new IllegalArgumentException();
        }
    }
}