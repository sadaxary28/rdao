package com.infomaximum.rocksdb;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.anotation.Index;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;

@Entity(
        namespace = "com.infomaximum.rocksdb",
        name = "record",
        fields = {
                @Field(name = RecordIndexReadable.FIELD_STRING_1, type = String.class),
                @Field(name = RecordIndexReadable.FIELD_LONG_1, type = Long.class),
                @Field(name = RecordIndexReadable.FIELD_INT_1, type = Integer.class),
                @Field(name = RecordIndexReadable.FIELD_BOOLEAN_1, type = Boolean.class)
        },
        indexes = {
                @Index(fields = {RecordIndexReadable.FIELD_STRING_1}),
                @Index(fields = {RecordIndexReadable.FIELD_LONG_1}),
                @Index(fields = {RecordIndexReadable.FIELD_LONG_1, RecordIndexReadable.FIELD_STRING_1})
        }
)
public class RecordIndexReadable  extends DomainObject {

    public final static String FIELD_STRING_1 = "string_1";
    public final static String FIELD_LONG_1 = "long_1";
    public final static String FIELD_INT_1 = "int_1";
    public final static String FIELD_BOOLEAN_1 ="boolean_1";

    public RecordIndexReadable(long id) {
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
