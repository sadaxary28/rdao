package com.infomaximum.database.domainobject.key;

/**
 * Created by kris on 27.04.17.
 */
public enum TypeKey {

    AVAILABILITY(1),

    FIELD(2),

    INDEX(3);

    private final int id;

    private TypeKey(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TypeKey get(int id) {
        for (TypeKey item: TypeKey.values()) {
            if (item.id==id) return item;
        }
        return null;
    }
}
