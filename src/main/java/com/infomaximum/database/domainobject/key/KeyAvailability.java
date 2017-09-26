package com.infomaximum.database.domainobject.key;

import com.infomaximum.database.utils.TypeConvert;

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
    public byte[] pack() {
        return TypeConvert.pack(new StringBuilder().append(packId(id)).append('.').append(getTypeKey().getId()).toString());
    }
}
