package com.infomaximum.rocksdb.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.rocksdb.domain.type.FormatType;

/**
 * Created by user on 19.04.2017.
 */
public class StoreFileEditable extends StoreFileReadable implements DomainObjectEditable {

    public StoreFileEditable(long id) throws DatabaseException {
        super(id);
    }


    public void setFileName(String fileName) {
        set(FIELD_FILE_NAME, fileName);
    }

    public void setContentType(String contentType) {
        set(FIELD_CONTENT_TYPE, contentType);
    }

    public void setSize(long size) {
        set(FIELD_SIZE, size);
    }

    public void setSingle(boolean single) {
        set(FIELD_SINGLE, single);
    }

    public void setFormat(FormatType format) {
        set(FIELD_FORMAT, format);
    }
}
