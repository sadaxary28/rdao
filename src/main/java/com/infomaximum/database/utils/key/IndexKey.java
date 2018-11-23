package com.infomaximum.database.utils.key;

public abstract class IndexKey extends Key{

    public static final int FIELDS_HASH_BYTE_SIZE = 4;
    public static final int INDEX_NAME_BYTE_SIZE = 3;
    public static final int ATTENDANT_BYTE_SIZE = INDEX_NAME_BYTE_SIZE + FIELDS_HASH_BYTE_SIZE;

    final byte[] fieldsHash;

    IndexKey(long id, byte[] fieldsHash) {
        super(id);
        checkFieldsHash(fieldsHash);
        this.fieldsHash = fieldsHash;
    }

    private static void checkFieldsHash(final byte[] fieldsHash) {
        if(fieldsHash == null || fieldsHash.length != FIELDS_HASH_BYTE_SIZE) {
            throw new IllegalArgumentException();
        }
    }
}