package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.PrefixIndex;
import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.infomaximum.database.utils.key.IndexKey.ATTENDANT_BYTE_SIZE;

public class PrefixIndexKey {

    private static final int BLOCK_NUMBER_BYTE_SIZE = Integer.BYTES;
    private static final byte LEXEME_TERMINATOR = 0;

    private final String lexeme;
    private final int blockNumber;
    private final byte[] fieldsHash;

    private PrefixIndexKey(String lexeme, int blockNumber, final byte[] fieldsHash) {
        this.lexeme = lexeme;
        this.blockNumber = blockNumber;
        this.fieldsHash = fieldsHash;
    }

    public PrefixIndexKey(String lexeme, final PrefixIndex index) {
        this(lexeme, 0, index.fieldsHash);
    }

    public String getLexeme() {
        return lexeme;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public static void incrementBlockNumber(byte[] key) {
        ByteBuffer buffer = TypeConvert.wrapBuffer(key);
        int number = buffer.getInt(key.length - BLOCK_NUMBER_BYTE_SIZE);
        if (number == Integer.MAX_VALUE) {
            throw new RuntimeException("Unexpected block number " + number);
        }
        buffer.putInt(key.length - BLOCK_NUMBER_BYTE_SIZE, number + 1);
    }

    public byte[] pack() {
        byte[] value = TypeConvert.pack(lexeme);

        return TypeConvert.allocateBuffer(ATTENDANT_BYTE_SIZE + value.length + 1 + BLOCK_NUMBER_BYTE_SIZE)
                .put(PrefixIndex.INDEX_NAME_BYTES)
                .put(fieldsHash)
                .put(value)
                .put(LEXEME_TERMINATOR)
                .putInt(blockNumber)
                .array();
    }

    public static PrefixIndexKey unpack(byte[] key) {
        byte[] fieldsHash = KeyUtils.getIndexFieldsHash(key);
        byte[] payload = new byte[key.length - ATTENDANT_BYTE_SIZE];
        System.arraycopy(key, ATTENDANT_BYTE_SIZE, payload, 0,key.length - ATTENDANT_BYTE_SIZE);
        int endIndex = ByteUtils.indexOf(PrefixIndexKey.LEXEME_TERMINATOR, payload);
        return new PrefixIndexKey(
                TypeConvert.unpackString(payload, 0, endIndex),
                TypeConvert.wrapBuffer(payload).getInt(endIndex + 1),
                fieldsHash
        );
    }

    public static KeyPattern buildKeyPatternForFind(final String word, final PrefixIndex index) {
        byte[] key = KeyUtils.buildKey(index.getIndexNameBytes(), index.fieldsHash, TypeConvert.pack(word));
        return new KeyPattern(key);
    }

    public static KeyPattern buildKeyPatternForEdit(final String lexeme, final PrefixIndex index) {
        byte[] payload = TypeConvert.pack(lexeme);
        payload = Arrays.copyOf(payload, payload.length + 1);
        payload[payload.length - 1] = LEXEME_TERMINATOR;

        byte[] key = KeyUtils.buildKey(index.getIndexNameBytes(), index.fieldsHash, payload);
        return new KeyPattern(key);
    }
}
