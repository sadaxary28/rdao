package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.exeption.runtime.KeyCorruptedException;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.Set;

public class FieldKey extends Key {

    private final String fieldName;

    public FieldKey(long id) {
        super(id);
        this.fieldName = null;
    }

    public FieldKey(long id, String fieldName) {
        super(id);
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public boolean isBeginningObject() {
        return fieldName == null;
    }

    @Override
    public byte[] pack() {
        final byte[] name = fieldName != null ? TypeConvert.pack(fieldName) : null;
        final ByteBuffer buffer = TypeConvert.allocateBuffer(ID_BYTE_SIZE + (name != null ? name.length : 0));

        buffer.putLong(getId());
        if (name != null) {
            buffer.put(name);
        }

        return buffer.array();
    }

    public static FieldKey unpack(final byte[] src) {
        if (src.length < ID_BYTE_SIZE) {
            throw new KeyCorruptedException(src);
        }

        long id = TypeConvert.unpackLong(src, 0);
        if (src.length == ID_BYTE_SIZE) {
            return new FieldKey(id);
        }

        return new FieldKey(id, TypeConvert.unpackString(src, ID_BYTE_SIZE, src.length - ID_BYTE_SIZE));
    }

    public static byte[] buildKeyPrefix(long id) {
        return TypeConvert.pack(id);
    }

    public static KeyPattern buildKeyPattern(long id) {
        return new KeyPattern(buildKeyPrefix(id));
    }

    public static KeyPattern buildKeyPattern(final Set<String> fields) {
        return new KeyPattern(buildInnerPatterns(fields));
    }

    public static KeyPattern buildKeyPattern(long id, final Set<String> fields) {
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
