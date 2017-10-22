package com.infomaximum.database.datasource.modifier;

import com.infomaximum.database.utils.TypeConvert;

/**
 * Created by kris on 18.05.17.
 */
public class ModifierSet extends Modifier {

    private final byte[] value;

    public ModifierSet(String columnFamily, final byte[] key, final byte[] value) {
        super(columnFamily, key);
        this.value = value;
    }

    public ModifierSet(String columnFamily, final byte[] key) {
        this(columnFamily, key, TypeConvert.EMPTY_BYTE_ARRAY);
    }

    public byte[] getValue() {
        return value;
    }
}
