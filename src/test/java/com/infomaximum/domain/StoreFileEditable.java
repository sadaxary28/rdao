package com.infomaximum.domain;

import com.infomaximum.database.domainobject.DomainObjectEditable;
import com.infomaximum.domain.type.FormatType;

import java.util.Date;

/**
 * Created by user on 19.04.2017.
 */
public class StoreFileEditable extends StoreFileReadable implements DomainObjectEditable {

    public StoreFileEditable(long id) {
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

    public void setFolderId(long folderId) {
        set(FIELD_FOLDER_ID, folderId);
    }

    public void setDouble(Double value) {
        set(FIELD_DOUBLE, value);
    }

    public void setDate(Date value) {
        set(FIELD_DATE, value);
    }
}
