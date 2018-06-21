package com.infomaximum.database.utils.key;

import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.exception.runtime.KeyCorruptedException;
import com.infomaximum.database.utils.ByteUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public class FieldKey extends Key {

    private final byte[] fieldName;

    public FieldKey(long id) {
        super(id);
        this.fieldName = TypeConvert.EMPTY_BYTE_ARRAY;
    }

    public FieldKey(long id, byte[] fieldName) {
        super(id);
        if (ByteUtils.isNullOrEmpty(fieldName)) {
            throw new IllegalArgumentException();
        }
        this.fieldName = fieldName;
    }

    public byte[] getFieldName() {
        return fieldName;
    }

    public boolean isBeginningObject() {
        return fieldName == null;
    }

    @Override
    public byte[] pack() {
        final ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE + fieldName.length);
        buffer.putLong(getId());
        buffer.put(fieldName);
        return buffer.array();
    }

    public static long unpackId(byte[] src) {
        return TypeConvert.unpackLong(src, 0);
    }

    public static boolean unpackBeginningObject(byte[] src) {
        return src.length == ID_BYTE_SIZE;
    }

    public static String unpackFieldName(byte[] src) {
        return TypeConvert.unpackString(src, ID_BYTE_SIZE, src.length - ID_BYTE_SIZE);
    }

    public static byte[] buildKeyPrefix(long id) {
        return TypeConvert.pack(id);
    }

    public static KeyPattern buildKeyPattern(final Set<String> fields) {
        return new KeyPattern(buildInnerPatterns(fields));
    }

    public static KeyPattern buildKeyPattern(long id, final Set<String> fields) {
        if (fields == null) {
            return new KeyPattern(buildKeyPrefix(id));
        }
        return new KeyPattern(buildKeyPrefix(id), buildInnerPatterns(fields));
    }

    private static KeyPattern.Postfix[] buildInnerPatterns(final Set<String> fields) {
        KeyPattern.Postfix[] patterns = new KeyPattern.Postfix[fields.size() + 1];

        patterns[0] = new KeyPattern.Postfix(ID_BYTE_SIZE, TypeConvert.EMPTY_BYTE_ARRAY);
        int i = 1;
        for (String field : fields) {
            patterns[i++] = new KeyPattern.Postfix(ID_BYTE_SIZE, TypeConvert.pack(field));
        }

        return patterns;
    }
}
