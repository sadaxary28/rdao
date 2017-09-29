package com.infomaximum.database.datasource;

import com.infomaximum.database.utils.ByteUtils;

import java.io.Serializable;

public class KeyPattern implements Serializable {

    public static final int MATCH_RESULT_SUCCESS = 1;
    public static final int MATCH_RESULT_CONTINUE = 0;
    public static final int MATCH_RESULT_UNSUCCESS = -1;

    public static class Postfix implements Serializable {
        private final int startPos;
        private final byte[] value;

        public Postfix(int startPos, byte[] value) {
            this.startPos = startPos;
            this.value = value;
        }

        public boolean match(final byte[] key) {
            if ((key.length - startPos) != value.length) {
                return false;
            }
            return ByteUtils.endsWith(value, key);
        }
    }

    private byte[] prefix;
    private final Postfix[] orPatterns;

    public KeyPattern(byte[] prefix, Postfix[] orPatterns) {
        this.prefix = prefix;
        this.orPatterns = orPatterns;
    }

    public KeyPattern(byte[] prefix) {
        this(prefix, null);
    }

    public KeyPattern(Postfix[] orPatterns) {
        this(null, orPatterns);
    }

    public void setPrefix(byte[] prefix) {
        this.prefix = prefix;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    public int match(final byte[] key) {
        if (prefix != null && !ByteUtils.startsWith(prefix, key)) {
            return MATCH_RESULT_UNSUCCESS;
        }

        if (orPatterns == null) {
            return MATCH_RESULT_SUCCESS;
        }

        for (int i = 0; i < orPatterns.length; ++i) {
            if (orPatterns[i].match(key)) {
                return MATCH_RESULT_SUCCESS;
            }
        }

        return MATCH_RESULT_CONTINUE;
    }
}
