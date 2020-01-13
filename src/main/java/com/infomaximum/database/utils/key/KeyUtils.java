package com.infomaximum.database.utils.key;

import com.infomaximum.database.exception.runtime.KeyCorruptedException;
import com.infomaximum.database.schema.BaseIndex;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.utils.TypeConvert;

import java.util.List;

import static com.infomaximum.database.schema.dbstruct.DBIndex.ATTENDANT_BYTE_SIZE;

public class KeyUtils {

    /**
     * Заполняет начало байтового массива названием индекса и хэшем индексируемых полей: [index_name][fields_hash]...
     */
    public static void putAttendantBytes(byte[] destination, final byte[] indexName, final byte[] fieldsHash) {
        if (destination.length < BaseIndex.ATTENDANT_BYTE_SIZE) {
            throw new IllegalArgumentException("Attendant size more than buffer size");
        }
        System.arraycopy(indexName, 0, destination, 0, indexName.length);
        System.arraycopy(fieldsHash, 0, destination, indexName.length, fieldsHash.length);
    }

    public static byte[] buildIndexAttendant(byte[] indexNameBytes, List<DBField> sortedIndexedFields) {
        byte[] result = new byte[ATTENDANT_BYTE_SIZE];
        KeyUtils.putAttendantBytes(result, indexNameBytes, buildFieldsHashCRC32(sortedIndexedFields));
        return result;
    }

    public static byte[] getIndexAttendant(byte[] src) {
        if (src.length < BaseIndex.ATTENDANT_BYTE_SIZE) {
            throw new KeyCorruptedException(src);
        }
        byte[] result = new byte[BaseIndex.ATTENDANT_BYTE_SIZE];
        System.arraycopy(src, 0, result, 0, BaseIndex.ATTENDANT_BYTE_SIZE);
        return result;
    }

    static byte[] allocateAndPutIndexAttendant(int size, byte[] attendant) {
        if (size < attendant.length) {
            throw new IllegalArgumentException("Attendant size more than buffer size");
        }
        byte[] result = new byte[size];
        System.arraycopy(attendant, 0, result, 0, attendant.length);
        return result;
    }

    private static byte[] buildFieldsHashCRC32(List<DBField> indexedFields) {
        StringBuilder stringBuilder = new StringBuilder();
        indexedFields.forEach(field -> stringBuilder.append(field.getName()).append(':').append(field.getType().getName()).append('.'));
        return TypeConvert.packCRC32(stringBuilder.toString());
    }
}