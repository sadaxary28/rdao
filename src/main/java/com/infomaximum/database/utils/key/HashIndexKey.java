package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.exception.runtime.KeyCorruptedException;
import com.infomaximum.database.schema.HashIndex;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HashIndexKey extends IndexKey {

    private final static String INDEX_NAME = "hsh";
    private final static byte[] INDEX_NAME_BYTES = INDEX_NAME.getBytes();
    private static final int INDEX_NAME_BYTE_SIZE = INDEX_NAME_BYTES.length;

    private final long[] fieldValues;

    public HashIndexKey(long id, final HashIndex index) {
        this(id, index.fieldsHash, new long[index.sortedFields.size()]);
    }

    public HashIndexKey(long id, final byte[] fieldsHash, final long[] fieldValues) {
        super(id, fieldsHash);

        if (fieldValues == null || fieldValues.length == 0 || fieldsHash == null || fieldsHash.length == 0) {
            throw new IllegalArgumentException();
        }
        this.fieldValues = fieldValues;
    }

    public long[] getFieldValues() {
        return fieldValues;
    }

    public byte[] getFieldsHash() {
        return fieldsHash;
    }

    @Override
    public byte[] pack() {
        byte[] buffer = new byte[ID_BYTE_SIZE + ID_BYTE_SIZE * fieldValues.length  + FIELDS_HASH_BYTE_SIZE + INDEX_NAME_BYTE_SIZE];
        System.arraycopy(INDEX_NAME_BYTES, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, fieldsHash.length);
        int offset = INDEX_NAME_BYTE_SIZE + fieldsHash.length;

        offset = TypeConvert.pack(fieldValues, buffer, offset);
        TypeConvert.pack(getId(), buffer, offset);
        return buffer;
    }

    public static HashIndexKey unpack(final byte[] src) {
        final int longCount = readLongCount(src);

        ByteBuffer buffer = TypeConvert.wrapBuffer(src);
        checkIndexNameAndIncreasePosition(buffer);

        byte[] fieldsHash = new byte[FIELDS_HASH_BYTE_SIZE];
        buffer.get(fieldsHash);

        long[] fieldValues = new long[longCount - 1];
        for (int i = 0; i < fieldValues.length; ++i) {
            fieldValues[i] = buffer.getLong();
        }
        return new HashIndexKey(buffer.getLong(), fieldsHash, fieldValues);
    }

    public static long unpackId(final byte[] src) {
        return TypeConvert.unpackLong(src, src.length - ID_BYTE_SIZE);
    }

    public static long unpackFirstIndexedValue(final byte[] src) {
        return TypeConvert.unpackLong(src, FIELDS_HASH_BYTE_SIZE + INDEX_NAME_BYTE_SIZE);
    }

    public static KeyPattern buildKeyPattern(final byte[] fieldsHash, final long[] fieldValues) {
        byte[] buffer = new byte[ID_BYTE_SIZE * fieldValues.length  + FIELDS_HASH_BYTE_SIZE + INDEX_NAME_BYTE_SIZE];
        System.arraycopy(INDEX_NAME_BYTES, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, fieldsHash.length);
        int offset = INDEX_NAME_BYTE_SIZE + fieldsHash.length;

        TypeConvert.pack(fieldValues, buffer, offset);
        return new KeyPattern(buffer);
    }

    public static KeyPattern buildKeyPattern(final HashIndex hashIndex, final long fieldValue) {
        byte[] buffer = new byte[Long.BYTES  + FIELDS_HASH_BYTE_SIZE + INDEX_NAME_BYTE_SIZE];
        System.arraycopy(INDEX_NAME_BYTES, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(hashIndex.fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, hashIndex.fieldsHash.length);
        int offset = INDEX_NAME_BYTE_SIZE + hashIndex.fieldsHash.length;

        TypeConvert.pack(fieldValue, buffer, offset);
        return new KeyPattern(buffer);
    }

    public static byte[] buildKeyPrefix(final byte[] fieldsHash) {
        byte[] buffer = new byte[FIELDS_HASH_BYTE_SIZE + INDEX_NAME_BYTE_SIZE];
        System.arraycopy(INDEX_NAME_BYTES, 0, buffer, 0, INDEX_NAME_BYTE_SIZE);
        System.arraycopy(fieldsHash, 0, buffer, INDEX_NAME_BYTE_SIZE, fieldsHash.length);
        return buffer;
    }

    private static int readLongCount(final byte[] src) {
        final int fieldsByteSize = src.length - (INDEX_NAME_BYTE_SIZE + FIELDS_HASH_BYTE_SIZE);
        final int count = fieldsByteSize / ID_BYTE_SIZE;
        final int tail = fieldsByteSize % ID_BYTE_SIZE;
        if (count < 2 || tail != 0) {
            throw new KeyCorruptedException(src);
        }
        return count;
    }

    private static void checkIndexNameAndIncreasePosition(ByteBuffer buffer) {
        byte[] indexName = new byte[INDEX_NAME_BYTE_SIZE];
        buffer.get(indexName);
        if (!Arrays.equals(INDEX_NAME_BYTES, indexName)) {
            throw new KeyCorruptedException("Invalid hash index key: key doesn't contains [index_name]: " + INDEX_NAME);
        }
    }
}
