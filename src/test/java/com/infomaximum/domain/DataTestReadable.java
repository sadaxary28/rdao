package com.infomaximum.domain;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.domainobject.DomainObject;

@Entity(
        namespace = "com.infomaximum.exchange",
        name = "dataTest",
        fields = {
                @Field(number = DataTestReadable.FIELD_VALUE, name = "value", type = String.class)
        }
)
public class DataTestReadable extends DomainObject {
    public final static int FIELD_VALUE = 0;

    public DataTestReadable(long id){
        super(id);
    }

    public String getValue() {
        return getString(FIELD_VALUE);
    }
}