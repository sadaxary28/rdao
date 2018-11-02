package com.infomaximum.rocksdb.deleteperfomance;

import com.infomaximum.database.domainobject.DomainObjectEditable;

public class GeneralEditable extends GeneralReadable implements DomainObjectEditable {

    public GeneralEditable(long id) {
        super(id);
    }

    void setValue(Long value) {
        set(FIELD_VALUE, value);
    }
}