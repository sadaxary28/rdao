package com.infomaximum.database.utils.key;

import com.infomaximum.database.exception.runtime.KeyCorruptedException;

import static com.infomaximum.database.schema.BaseIndex.ATTENDANT_BYTE_SIZE;
import static com.infomaximum.database.schema.BaseIndex.FIELDS_HASH_BYTE_SIZE;
import static com.infomaximum.database.schema.BaseIndex.INDEX_NAME_BYTE_SIZE;

public class KeyUtils {

    /**
     * Заполняет начало байтового массива названием индекса и хэшем индексируемых полей: [index_name][fields_hash]...
     */
    public static void putAttendantBytes(byte[] buffer, final byte[] indexName, final byte[] fieldsHash) {
        if (buffer.length < ATTENDANT_BYTE_SIZE) {
            throw new IllegalArgumentException("Attendant size more than buffer size");
        }
        System.arraycopy(indexName, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, FIELDS_HASH_BYTE_SIZE);
    }

    static byte[] allocateAndPutIndexAttendant(int size, byte[] attendant) {
        if (attendant.length < ATTENDANT_BYTE_SIZE || size < attendant.length) {
            throw new IllegalArgumentException("Attendant size more than buffer size");
        }
        byte[] result = new byte[size];
        System.arraycopy(attendant, 0, result, 0, ATTENDANT_BYTE_SIZE);
        return result;
    }

    public static byte[] getIndexAttendant(byte[] src) {
        if (src.length < ATTENDANT_BYTE_SIZE) {
            throw new KeyCorruptedException(src);
        }
        byte[] result = new byte[ATTENDANT_BYTE_SIZE];
        System.arraycopy(src, 0, result, 0, ATTENDANT_BYTE_SIZE);
        return result;
    }
}