package com.infomaximum.rocksdb.domain;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectReadable;

import java.util.Date;

/**
 * Created by kris on 27.06.17.
 */
@Entity(
        name = "com.infomaximum.exchange.ExchangeFolder",
        fields = {
                @Field(name = ExchangeFolderReadable.FIELD_UUID, type = String.class),
                @Field(name = ExchangeFolderReadable.FIELD_USER_EMAIL, type = String.class),
                @Field(name = ExchangeFolderReadable.FIELD_SYNC_DATE, type = Date.class),
                @Field(name = ExchangeFolderReadable.FIELD_SYNC_STATE, type = String.class),
        },
        indexes = {
                @Index(fields = {ExchangeFolderReadable.FIELD_USER_EMAIL, ExchangeFolderReadable.FIELD_UUID})
        }
)
public class ExchangeFolderReadable extends DomainObject implements DomainObjectReadable {

    protected final static String FIELD_UUID="uuid";
    protected final static String FIELD_USER_EMAIL="user_email";
    protected final static String FIELD_SYNC_DATE="sync_date";
    protected final static String FIELD_SYNC_STATE="sync_state";

    public ExchangeFolderReadable(long id) {
        super(id);
    }

    public String getUuid() {
        return getString(FIELD_UUID);
    }

    public String getUserEmail() {
        return getString(FIELD_USER_EMAIL);
    }

    public Date getSyncDate() {
        return getDate(FIELD_SYNC_DATE);
    }

    public String getSyncState() {
        return getString(FIELD_SYNC_STATE);
    }

//
//    public void setUuid(String uuid) {
//        this.uuid = uuid;
//    }
//
//    public void setUserEmail(String userEmail) {
//        this.userEmail = userEmail;
//    }
//
//    public void setSyncDate(Date syncDate) {
//        this.syncDate = syncDate;
//    }
//
//    public void setSyncState(String syncState) {
//        this.syncState = syncState;
//    }
}