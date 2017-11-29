package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class PrefixIndexKey {

    public static final int FIRST_BLOCK_NUMBER = Integer.MAX_VALUE;
    public static final int BLOCK_NUMBER_BYTE_SIZE = Integer.BYTES;
    public static final byte LEXEME_TERMINATOR = 0;

    private final String lexeme;
    private final int blockNumber;

    private PrefixIndexKey(String lexeme, int blockNumber) {
        this.lexeme = lexeme;
        this.blockNumber = blockNumber;
    }

    public PrefixIndexKey(String lexeme) {
        this(lexeme, FIRST_BLOCK_NUMBER);
    }

    public String getLexeme() {
        return lexeme;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public static void decrementBlockNumber(byte[] key) {
        ByteBuffer buffer = TypeConvert.wrapBuffer(key);
        int number = buffer.getInt(key.length - BLOCK_NUMBER_BYTE_SIZE);
        if (number < 1) {
            throw new RuntimeException("Unexpected block number " + number);
        }
        buffer.putInt(key.length - BLOCK_NUMBER_BYTE_SIZE, number - 1);
    }

    public byte[] pack() {
        byte[] value = TypeConvert.pack(lexeme);

        return TypeConvert.allocateBuffer(value.length + 1 + BLOCK_NUMBER_BYTE_SIZE)
                .put(value)
                .put(LEXEME_TERMINATOR)
                .putInt(blockNumber)
                .array();
    }

    public static PrefixIndexKey unpack(byte[] key) {
        int endIndex = ByteUtils.indexOf(PrefixIndexKey.LEXEME_TERMINATOR, key);
        return new PrefixIndexKey(
                TypeConvert.unpackString(key, 0, endIndex),
                TypeConvert.wrapBuffer(key).getInt(endIndex + 1)
        );
    }

    public static KeyPattern buildKeyPatternForFind(final String word) {
        return new KeyPattern(TypeConvert.pack(word));
    }

    public static KeyPattern buildKeyPatternForEdit(final String lexeme) {
        byte[] bytes = TypeConvert.pack(lexeme);
        bytes = Arrays.copyOf(bytes, bytes.length + 1);
        //COMMENT last byte must contains zero value
        return new KeyPattern(bytes);
    }
}
