package com.infomaximum.rocksdb.core.objectsource.utils.key;

/**
 * Created by kris on 27.04.17.
 */
public abstract class Key {

    public final long id;

    public Key(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public abstract TypeKey getTypeKey();

    public abstract String pack();

    public static Key parse(String sKey) {
        String[] keySplit = sKey.split("\\.", 3);

        long id = Long.parseLong(keySplit[0]);
        TypeKey typeKey = TypeKey.get(Integer.parseInt(keySplit[1]));

        if (typeKey==TypeKey.AVAILABILITY) {
            return new KeyAvailability(id);
        } else if (typeKey==TypeKey.FIELD) {
            return new KeyField(id, keySplit[2]);
        } else {
            throw new RuntimeException("Not support type enum");
        }
    }
}
