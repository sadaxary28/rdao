package com.infomaximum.rocksdb.domain;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.anotation.Index;
import com.infomaximum.rocksdb.core.struct.DomainObject;

import java.util.Date;

/**
 * Created by kris on 27.06.17.
 */
@Entity(
        columnFamily = "com.infomaximum.exchange.ExchangeFolder",
        indexes = {
                @Index(fieldNames = {"userEmail", "uuid"})
        }
)
public class ExchangeFolder extends DomainObject {

    @EntityField
    private String uuid;

    @EntityField
    private String userEmail;

    @EntityField
    private Date syncDate;

    @EntityField
    private String syncState;

    public ExchangeFolder(long id) {
        super(id);
    }

    public String getUuid() {
        return uuid;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public Date getSyncDate() {
        return syncDate;
    }

    public String getSyncState() {
        return syncState;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public void setSyncDate(Date syncDate) {
        this.syncDate = syncDate;
    }

    public void setSyncState(String syncState) {
        this.syncState = syncState;
    }
}