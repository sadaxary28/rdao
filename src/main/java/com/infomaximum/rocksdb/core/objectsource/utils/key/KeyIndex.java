package com.infomaximum.rocksdb.core.objectsource.utils.key;

/**
 * Created by kris on 27.04.17.
 */
public class KeyIndex extends Key {

    protected static String PREFIX = "i.";

    private final String index;
    private final int hash;

    public KeyIndex(long id, String index, int hash) {
        super(id);
        this.index=index;
        this.hash=hash;
    }

    @Override
    public TypeKey getTypeKey() {
        return TypeKey.INDEX;
    }

    public String getIndex() {
        return index;
    }

    public int getHash() {
        return hash;
    }

    @Override
    public String pack() {
        return prifix(index, hash) + packId(id);
    }

    public static String prifix(String index, int hash) {
        return new StringBuilder().append(PREFIX).append(index).append('.').append(hash).append('.').toString();
    }
}
