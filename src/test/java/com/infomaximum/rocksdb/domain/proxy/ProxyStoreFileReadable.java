package com.infomaximum.rocksdb.domain.proxy;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.rocksdb.domain.type.FormatType;

/**
 * Created by user on 19.04.2017.
 */
@Entity(
        namespace = "com.infomaximum",
        name = "StoreFile",
        fields = {
                @Field(name = ProxyStoreFileReadable.FIELD_FILE_NAME, type = String.class),
                @Field(name = ProxyStoreFileReadable.FIELD_CONTENT_TYPE, type = String.class),
                @Field(name = ProxyStoreFileReadable.FIELD_SIZE, type = Long.class),
                @Field(name = ProxyStoreFileReadable.FIELD_SINGLE, type = Boolean.class),
                @Field(name = ProxyStoreFileReadable.FIELD_FORMAT, type = FormatType.class)
        },
        indexes = {
                @Index(fields = {ProxyStoreFileReadable.FIELD_SIZE}),
                @Index(fields = {ProxyStoreFileReadable.FIELD_SIZE, ProxyStoreFileReadable.FIELD_FILE_NAME})
        }
)
public class ProxyStoreFileReadable extends ProxyDomainObject {

    public final static String FIELD_FILE_NAME = "file_name";
    public final static String FIELD_CONTENT_TYPE = "content_type";
    public final static String FIELD_SIZE = "size";
    public final static String FIELD_SINGLE = "single";
    public final static String FIELD_FORMAT = "format";


    public ProxyStoreFileReadable(long id) {
        super(id);
    }


    public String getFileName() {
        return getString(FIELD_FILE_NAME);
    }

    public String getContentType() {
        return getString(FIELD_CONTENT_TYPE);
    }

    public long getSize() {
        return getLong(FIELD_SIZE);
    }

    public boolean isSingle() {
        return getBoolean(FIELD_SINGLE);
    }


    public FormatType getFormat() {
        return getEnum(FormatType.class, FIELD_FORMAT);
    }

}
