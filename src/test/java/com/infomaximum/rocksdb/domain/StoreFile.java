package com.infomaximum.rocksdb.domain;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.anotation.Index;
import com.infomaximum.rocksdb.core.struct.DomainObject;

/**
 * Created by user on 19.04.2017.
 */
@Entity(
        columnFamily = "com.infomaximum.StoreFile",
        indexes = {
                @Index(fieldNames = {"size"}),
                @Index(fieldNames = {"size", "fileName"})
        }
)
public class StoreFile extends DomainObject {

    @EntityField
    private String fileName;

    @EntityField
    private String contentType;

    @EntityField
    private long size;

    @EntityField
    private boolean single;

    public StoreFile(long id) {
        super(id);
    }

    public String getFileName() {
        return fileName;
    }
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getContentType() {
        return contentType;
    }
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getSize() {
        return size;
    }
    public void setSize(long size) {
        this.size = size;
    }


    public boolean isSingle() {
        return single;
    }
    public void setSingle(boolean single) {
        this.single = single;
    }
}
