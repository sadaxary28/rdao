package com.infomaximum.rocksdb;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DatabaseException;

@Entity(
        name = "com.infomaximum.rocksdb.record",
        fields = {
                @Field(name = RecordReadable.FIELD_STRING_1, type = String.class),
                @Field(name = RecordReadable.FIELD_LONG_1, type = Long.class),
                @Field(name = RecordReadable.FIELD_INT_1, type = Integer.class),
                @Field(name = RecordReadable.FIELD_BOOLEAN_1, type = Boolean.class)
        }
)
public class RecordReadable  extends DomainObject {

    public final static String FIELD_STRING_1 = "string_1";
    public final static String FIELD_LONG_1 = "long_1";
    public final static String FIELD_INT_1 = "int_1";
    public final static String FIELD_BOOLEAN_1 ="boolean_1";

    public RecordReadable(long id) {
        super(id);
    }

    public String getString1() throws DatabaseException {
        return getString(FIELD_STRING_1);
    }

    public long getLong1() throws DatabaseException {
        return getLong(FIELD_LONG_1);
    }

    public boolean getBoolean1() throws DatabaseException {
        return getBoolean(FIELD_BOOLEAN_1);
    }

    public int getInt1() throws DatabaseException {
        return getInteger(FIELD_INT_1);
    }
}

