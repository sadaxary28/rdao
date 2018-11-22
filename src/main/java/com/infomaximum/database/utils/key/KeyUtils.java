package com.infomaximum.database.utils.key;

import com.infomaximum.database.exception.runtime.KeyCorruptedException;

import static com.infomaximum.database.utils.key.IndexKey.ATTENDANT_BYTE_SIZE;
import static com.infomaximum.database.utils.key.IndexKey.FIELDS_HASH_BYTE_SIZE;
import static com.infomaximum.database.utils.key.IndexKey.INDEX_NAME_BYTE_SIZE;

public class KeyUtils {

    /**
     * Заполняет начало байтового массива названием индекса и хэшем индексируемых полей: [index_name][fields_hash]...
     * @return offset
     */
    public static int putAttendantBytes(byte[] buffer, final byte[] indexName, final byte[] fieldsHash) {
        if (buffer.length < ATTENDANT_BYTE_SIZE) {
            throw new IllegalArgumentException("Attendant size more than buffer size");
        }
        System.arraycopy(indexName, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, FIELDS_HASH_BYTE_SIZE);
        return ATTENDANT_BYTE_SIZE;
    }

    /**
     * @return байтовый массив с названием индекса, хэшем индексируемых полей и значениями индекса: [index_name][fields_hash][values]
     */
    public static byte[] buildKey(final byte[] indexName, final byte[] fieldsHash, final byte[] payload) {
        byte[] result = new byte[payload.length + ATTENDANT_BYTE_SIZE];
        int payloadDestPos = putAttendantBytes(result, indexName, fieldsHash);
        System.arraycopy(payload, 0, result, payloadDestPos, payload.length);
        return result;
    }

    public static byte[] getIndexFieldsHash(byte[] src) {
        if (src.length < ATTENDANT_BYTE_SIZE) {
            throw new KeyCorruptedException(src);
        }
        byte[] result = new byte[src.length - ATTENDANT_BYTE_SIZE];
        System.arraycopy(src, INDEX_NAME_BYTE_SIZE, result, 0, FIELDS_HASH_BYTE_SIZE);
        return result;
    }
}
