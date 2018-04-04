package com.infomaximum.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;
import com.infomaximum.database.exception.DatabaseException;

import java.util.Date;

/**
 * Created by kris on 27.06.17.
 */
public class ExchangeFolderEditable extends ExchangeFolderReadable implements DomainObjectEditable {

    public ExchangeFolderEditable(long id){
        super(id);
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

    public void setParentId(Long parentId) throws DatabaseException {
        set(FIELD_PARENT_ID, parentId);
    }
}