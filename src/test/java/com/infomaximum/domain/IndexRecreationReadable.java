package com.infomaximum.domain;

import com.infomaximum.database.anotation.*;
import com.infomaximum.database.domainobject.DomainObject;

import java.time.Instant;

@Entity(
        namespace = "com.infomaximum.rocksdb",
        name = "IndexRecreation",
        fields = {
                @Field(number = IndexRecreationReadable.FIELD_NAME_Z, name = "z_name", type = String.class),
                @Field(number = IndexRecreationReadable.FIELD_TYPE, name = "a_type", type = Boolean.class),
                @Field(number = IndexRecreationReadable.FIELD_S_BEGIN, name = "s_begin", type = Instant.class),
                @Field(number = IndexRecreationReadable.FIELD_AMOUNT, name = "a_amount", type = Long.class),
                @Field(number = IndexRecreationReadable.FIELD_NAME_X, name = "x_name", type = String.class),
                @Field(number = IndexRecreationReadable.FIELD_PRICE, name = "a_price", type = Long.class),
                @Field(number = IndexRecreationReadable.FIELD_G_END, name = "g_end", type = Instant.class),
        },
        hashIndexes = {
                @HashIndex(fields = {IndexRecreationReadable.FIELD_PRICE, IndexRecreationReadable.FIELD_NAME_Z})
        },

        prefixIndexes = {
                @PrefixIndex(fields = {IndexRecreationReadable.FIELD_NAME_X, IndexRecreationReadable.FIELD_NAME_Z})
        },

        intervalIndexes = {
                @IntervalIndex(indexedField = IndexRecreationReadable.FIELD_AMOUNT,
                        hashedFields = {IndexRecreationReadable.FIELD_PRICE, IndexRecreationReadable.FIELD_NAME_Z})
        },
        rangeIndexes = {
                @RangeIndex(beginField = IndexRecreationReadable.FIELD_S_BEGIN, endField = IndexRecreationReadable.FIELD_G_END,
                        hashedFields = {IndexRecreationReadable.FIELD_PRICE, IndexRecreationReadable.FIELD_NAME_Z}
                )
        }
)
public class IndexRecreationReadable extends DomainObject {

    public final static int FIELD_NAME_Z = 0;
    public final static int FIELD_TYPE = 1;
    public final static int FIELD_S_BEGIN = 2;
    public final static int FIELD_AMOUNT = 3;
    public final static int FIELD_NAME_X = 4;
    public final static int FIELD_PRICE = 5;
    public final static int FIELD_G_END = 6;

    public IndexRecreationReadable(long id) {
        super(id);
    }

    public String getNameZ() {
        return getString(FIELD_NAME_Z);
    }

    public Boolean getType() {
        return getBoolean(FIELD_TYPE);
    }

    public Instant getBegin() {
        return getInstant(FIELD_S_BEGIN);
    }

    public Long getAmount() {
        return getLong(FIELD_AMOUNT);
    }

    public String getNameX() {
        return getString(FIELD_NAME_X);
    }

    public Long getPrice() {
        return getLong(FIELD_PRICE);
    }

    public Instant getEnd() {
        return getInstant(FIELD_G_END);
    }
}