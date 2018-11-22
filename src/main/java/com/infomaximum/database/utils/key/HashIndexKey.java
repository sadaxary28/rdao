package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.exception.runtime.KeyCorruptedException;
import com.infomaximum.database.schema.HashIndex;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HashIndexKey extends IndexKey {

    private final long[] fieldValues;

    public HashIndexKey(long id, final HashIndex index) {
        this(id, index.fieldsHash, new long[index.sortedFields.size()]);
    }

    public HashIndexKey(long id, final byte[] fieldsHash, final long[] fieldValues) {
        super(id, fieldsHash);

        if (fieldValues == null || fieldValues.length == 0) {
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
        byte[] payload = new byte[ID_BYTE_SIZE * fieldValues.length + ID_BYTE_SIZE];
        int offset = TypeConvert.pack(fieldValues, payload, 0);
        TypeConvert.pack(getId(), payload, offset);
        return KeyUtils.buildKey(HashIndex.INDEX_NAME_BYTES, fieldsHash, payload);
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
        return TypeConvert.unpackLong(src, INDEX_NAME_BYTE_SIZE + FIELDS_HASH_BYTE_SIZE);
    }

    public static KeyPattern buildKeyPattern(final HashIndex hashIndex, final long[] fieldValues) {
        byte[] payload = new byte[ID_BYTE_SIZE * fieldValues.length];
        TypeConvert.pack(fieldValues, payload, 0);
        return new KeyPattern(KeyUtils.buildKey(hashIndex.getIndexNameBytes(), hashIndex.fieldsHash, payload));
    }

    public static KeyPattern buildKeyPattern(final HashIndex hashIndex, final long fieldValue) {
        byte[] payload = new byte[ID_BYTE_SIZE];
        TypeConvert.pack(fieldValue, payload, 0);
        return new KeyPattern(KeyUtils.buildKey(hashIndex.getIndexNameBytes(), hashIndex.fieldsHash, payload));
    }

    public static KeyPattern buildKeyPatternForLastKey(final HashIndex hashIndex) {
        byte[] payload = new byte[ID_BYTE_SIZE];
        TypeConvert.pack(0xFFFFFFFFFFFFFFFFL, payload, 0);
        byte[] key = KeyUtils.buildKey(hashIndex.getIndexNameBytes(), hashIndex.fieldsHash, payload);
        return new KeyPattern(key, ATTENDANT_BYTE_SIZE);
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
        if (!Arrays.equals(HashIndex.INDEX_NAME_BYTES, indexName)) {
            throw new KeyCorruptedException("Invalid hash index key: key doesn't contains [index_name]: " + HashIndex.INDEX_NAME);
        }
    }
}
