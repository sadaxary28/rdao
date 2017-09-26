package com.infomaximum.database.core.transaction.modifier;

import com.infomaximum.database.domainobject.key.Key;

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
