package com.infomaximum.rocksdb.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;

import java.util.Date;

/**
 * Created by kris on 27.06.17.
 */
public class ExchangeFolderEditable extends ExchangeFolderReadable implements DomainObjectEditable {


    public ExchangeFolderEditable(long id) {
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


    public void setUuid(String uuid) {
        set(FIELD_UUID, uuid);
    }

    public void setUserEmail(String userEmail) {
        set(FIELD_USER_EMAIL, userEmail);
    }

    public void setSyncDate(Date syncDate) {
        set(FIELD_SYNC_DATE, syncDate);
    }

    public void setSyncState(String syncState) {
        set(FIELD_SYNC_STATE, syncState);
    }
}