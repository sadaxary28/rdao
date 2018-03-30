package com.infomaximum.database.utils;

import com.google.common.primitives.UnsignedInts;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.TypeConverter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class IndexUtils {

    public static boolean toLongCastable(Class<?> type) {
        return type != String.class;
    }

    public static void setHashValues(final List<EntityField> sortedFields, final Map<EntityField, Serializable> values, long[] destination) {
        for (int i = 0; i < sortedFields.size(); ++i) {
            EntityField field = sortedFields.get(i);
            destination[i] = buildHash(field.getType(), values.get(field), field.getConverter());
        }
    }

    public static void setHashValues(final List<EntityField> sortedFields, final DomainObject object, long[] destination) throws DatabaseException {
        for (int i = 0; i < sortedFields.size(); ++i) {
            EntityField field = sortedFields.get(i);
            destination[i] = buildHash(field.getType(), object.get(field.getName()), field.getConverter());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> long buildHash(Class<T> type, Object value, TypeConverter<T> converter) {
        if (converter != null) {
            return converter.buildHash((T)value);
        }

        if (value == null) {
            return 0;
        }

        if (type == Long.class) {
            return (Long) value;
        } else if (type == String.class) {
            return hash(TypeConvert.pack(((String) value).toLowerCase()));
        } else if (type == Boolean.class) {
            return ((Boolean) value) ? 1 : 0;
        } else if (type == Date.class) {
            return ((Date) value).getTime();
        } else if (type == Integer.class) {
            return UnsignedInts.toLong((Integer) value);
        }
        throw new IllegalTypeException("Unsupported type " + type + " for hashing.");
    }

    public static boolean equals(Class<?> clazz, Object left, Object right) {
        if (left == null) {
            return right == null;
        }

        if (clazz == String.class) {
            return ((String)left).equalsIgnoreCase((String)right);
        }

        return left.equals(right);
    }

    /**
     * http://www.azillionmonkeys.com/qed/hash.html
     */
    private static long hash(final byte[] data) {
        if (data == null) {
            return 0;
        }

        int len = data.length;
        int hash = len;
        int tmp, rem;

        rem = len & 3;
        len >>>= 2;

        /* Main loop */
        int pos = 0;
        for (; len > 0; --len) {
            hash += get16bits(data, pos);
            tmp = (get16bits(data, pos + 2) << 11) ^ hash;
            hash = ((hash << 16) ^ tmp);
            hash += (hash >>> 11);
            pos += 4;
        }

        /* Handle end cases */
        switch (rem) {
            case 3:
                hash += get16bits(data, pos);
                hash ^= (hash << 16);
                hash ^= ((data[pos + 2] & 0xff) << 18);
                hash += (hash >>> 11);
                break;
            case 2:
                hash += get16bits(data, pos);
                hash ^= (hash << 11);
                hash += (hash >>> 17);
                break;
            case 1:
                hash += (data[pos] & 0xff);
                hash ^= (hash << 10);
                hash += (hash >>> 1);
                break;
        }

        /* Force "avalanching" of final 127 bits */
        hash ^= (hash << 3);
        hash += (hash >>> 5);
        hash ^= (hash << 4);
        hash += (hash >>> 17);
        hash ^= (hash << 25);
        hash += (hash >>> 6);

        return UnsignedInts.toLong(hash);
    }

    private static int get16bits(byte[] data, int startIndex) {
        return (data[startIndex] & 0xff) + ((data[startIndex + 1] << 8) & 0xffff);
    }
}
