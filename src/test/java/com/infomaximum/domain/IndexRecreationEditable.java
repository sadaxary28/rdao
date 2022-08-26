package com.infomaximum.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;

import java.time.Instant;

public class IndexRecreationEditable extends IndexRecreationReadable implements DomainObjectEditable {
    public IndexRecreationEditable(long id) {
        super(id);
    }

    public void setNameZ(String name) {
        set(FIELD_NAME_Z, name);
    }

    public void setType(Boolean type) {
        set(FIELD_TYPE, type);
    }

    public void setBegin(Instant begin) {
        set(FIELD_S_BEGIN, begin);
    }

    public void setNameX(String name) {
        set(FIELD_NAME_X, name);
    }

    public void setAmount(Long amount) {
        set(FIELD_AMOUNT, amount);
    }

    public void setPrice(Long price) {
        set(FIELD_PRICE, price);
    }

    public void setEnd(Instant end) {
        set(FIELD_G_END, end);
    }
}