package com.infomaximum.rocksdb.transaction.struct.modifier;

/**
 * Created by kris on 18.05.17.
 */
public class ModifierRemove extends Modifier  {

    public ModifierRemove(String columnFamily, String key) {
        super(columnFamily, key);
    }
}
