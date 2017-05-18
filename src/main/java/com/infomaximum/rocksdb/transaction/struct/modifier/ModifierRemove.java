package com.infomaximum.rocksdb.transaction.struct.modifier;

import com.infomaximum.rocksdb.core.objectsource.utils.key.Key;

/**
 * Created by kris on 18.05.17.
 */
public class ModifierRemove extends Modifier  {

    public ModifierRemove(String columnFamily, String key) {
        super(columnFamily, key);
    }

    public static ModifierRemove removeDomainObject(String columnFamily, long id) {
        String key = Key.getPatternObject(id) + '*';
        return new ModifierRemove(columnFamily, key);
    }
}
