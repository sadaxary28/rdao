package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.domainobject.DomainObjectEditable;

public class DependentEditable extends DependentReadable implements DomainObjectEditable {

    public DependentEditable(long id) {
        super(id);
    }

    void setName(String name) {
        set(FIELD_NAME, name);
    }

    void setGeneralId(Long generalId) {
        set(FIELD_GENERAL_ID, generalId);
    }
}