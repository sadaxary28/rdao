package com.infomaximum.domain;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.anotation.HashIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;

import java.time.Instant;

/**
 * Created by kris on 27.06.17.
 */
@Entity(
        namespace = "com.infomaximum.exchange",
        name = "ExchangeFolder",
        fields = {
                @Field(number = ExchangeFolderReadable.FIELD_UUID, name = "uuid", type = String.class),
                @Field(number = ExchangeFolderReadable.FIELD_USER_EMAIL, name = "email", type = String.class),
                @Field(number = ExchangeFolderReadable.FIELD_SYNC_DATE, name = "date", type = Instant.class),
                @Field(number = ExchangeFolderReadable.FIELD_SYNC_STATE, name = "state", type = String.class),
                @Field(number = ExchangeFolderReadable.FIELD_PARENT_ID, name = "parent_id", type = Long.class, foreignDependency = ExchangeFolderReadable.class),
        },
        hashIndexes = {
                @HashIndex(fields = {ExchangeFolderReadable.FIELD_USER_EMAIL, ExchangeFolderReadable.FIELD_UUID})
        }
)
public class ExchangeFolderReadable extends DomainObject {

    public final static int FIELD_UUID = 0;
    public final static int FIELD_USER_EMAIL = 1;
    public final static int FIELD_SYNC_DATE = 2;
    public final static int FIELD_SYNC_STATE = 3;
    public final static int FIELD_PARENT_ID = 4;

    public ExchangeFolderReadable(long id){
        super(id);
    }

    public String getUuid() throws DatabaseException {
        return getString(FIELD_UUID);
    }

    public String getUserEmail() throws DatabaseException {
        return getString(FIELD_USER_EMAIL);
    }

    public Instant getSyncDate() throws DatabaseException {
        return getInstant(FIELD_SYNC_DATE);
    }

    public String getSyncState() throws DatabaseException {
        return getString(FIELD_SYNC_STATE);
    }

    public Long getParentId() throws DatabaseException {
        return getLong(FIELD_PARENT_ID);
    }
}