package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.utils.TypeConvert;

/**
 * Created by kris on 27.04.17.
 */
public class KeyIndex extends Key {

    protected static String PREFIX = "i.";

    private final int hash;

    public KeyIndex(long id, int hash) {
        super(id);
        this.hash=hash;
    }

    @Override
    public TypeKey getTypeKey() {
        return TypeKey.INDEX;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public byte[] pack() {
        return TypeConvert.pack(prifix(hash) + packId(id));
    }

    public static String prifix(int hash) {
        return new StringBuilder().append(PREFIX).append(hash).append('.').toString();
    }
}
