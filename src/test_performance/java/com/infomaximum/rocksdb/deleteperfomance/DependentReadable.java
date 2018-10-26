package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.domainobject.DomainObject;

@Entity(
        namespace = "com.infomaximum.store",
        name = "dependent",
        fields = {
                @Field(number = DependentReadable.FIELD_NAME, name = "name", type = String.class),
                @Field(number = DependentReadable.FIELD_GENERAL_ID, name = "general_id", type = Long.class, foreignDependency = GeneralReadable.class)
        }
)
public class DependentReadable extends DomainObject {

    public final static int FIELD_NAME = 0;
    public final static int FIELD_GENERAL_ID = 1;

    public DependentReadable(long id) {
        super(id);
    }

    public String getName() {
        return getString(FIELD_NAME);
    }

    public long getGeneralId() {
        return getLong(FIELD_GENERAL_ID);
    }
}