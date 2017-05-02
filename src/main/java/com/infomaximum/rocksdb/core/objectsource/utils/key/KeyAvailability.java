package com.infomaximum.rocksdb.core.objectsource.utils.key;

/**
 * Created by kris on 27.04.17.
 */
public class KeyAvailability extends Key {

    public KeyAvailability(long id) {
        super(id);
    }

    @Override
    public TypeKey getTypeKey() {
        return TypeKey.AVAILABILITY;
    }

    @Override
    public String pack() {
        return new StringBuilder().append(packId(id)).append('.').append(getTypeKey().getId()).toString();
    }
}
