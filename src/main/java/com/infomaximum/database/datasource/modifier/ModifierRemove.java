package com.infomaximum.database.datasource.modifier;

/**
 * Created by kris on 18.05.17.
 */
public class ModifierRemove extends Modifier  {

    private final boolean isKeyPrefix;

    public ModifierRemove(String columnFamily, final byte[] key, boolean isKeyPrefix) {
        super(columnFamily, key);
        this.isKeyPrefix = isKeyPrefix;
    }

    public boolean isKeyPrefix() {
        return isKeyPrefix;
    }
}
