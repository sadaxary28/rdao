package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.anotation.HashIndex;
import com.infomaximum.database.domainobject.DomainObject;

@Entity(
        namespace = "com.infomaximum.rocksdb",
        name = "general",
        fields = {
                @Field(number = GeneralReadable.FIELD_VALUE, name = "value", type = Long.class),
        },
        hashIndexes = {
                @HashIndex(fields = {GeneralReadable.FIELD_VALUE})
        }
)
public class GeneralReadable extends DomainObject {

    public final static int FIELD_VALUE = 0;

    public GeneralReadable(long id) {
        super(id);
    }

    public Long getValue() {
        return getLong(FIELD_VALUE);
    }
}