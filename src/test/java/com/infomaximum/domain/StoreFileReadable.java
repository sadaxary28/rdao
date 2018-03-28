package com.infomaximum.domain;

import com.infomaximum.database.anotation.Entity;
import com.infomaximum.database.anotation.Field;
import com.infomaximum.database.anotation.Index;
import com.infomaximum.database.anotation.IntervalIndex;
import com.infomaximum.database.domainobject.DomainObject;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.EnumConverter;
import com.infomaximum.domain.type.FormatType;

import java.util.Date;

/**
 * Created by user on 19.04.2017.
 */
@Entity(
        namespace = "com.infomaximum.store",
        name = "StoreFile",
        fields = {
                @Field(name = StoreFileReadable.FIELD_FILE_NAME, type = String.class),
                @Field(name = StoreFileReadable.FIELD_CONTENT_TYPE, type = String.class),
                @Field(name = StoreFileReadable.FIELD_SIZE, type = Long.class),
                @Field(name = StoreFileReadable.FIELD_SINGLE, type = Boolean.class),
                @Field(name = StoreFileReadable.FIELD_FORMAT, type = FormatType.class, packerType = StoreFileReadable.FormatConverter.class),
                @Field(name = StoreFileReadable.FIELD_FOLDER_ID, type = Long.class, foreignDependency = ExchangeFolderReadable.class),
                @Field(name = StoreFileReadable.FIELD_DOUBLE, type = Double.class),
                @Field(name = StoreFileReadable.FIELD_DATE, type = Date.class)
        },
        indexes = {
                @Index(fields = {StoreFileReadable.FIELD_SIZE}),
                @Index(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @Index(fields = {StoreFileReadable.FIELD_SIZE, StoreFileReadable.FIELD_FILE_NAME}),
                @Index(fields = {StoreFileReadable.FIELD_FORMAT})
        },
        prefixIndexes = {
                @Index(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @Index(fields = {StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE})
        },
        intervalIndexes = {
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_DOUBLE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_DATE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FILE_NAME}),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID})
        }
)
public class StoreFileReadable extends DomainObject {

    public final static String FIELD_FILE_NAME="file_name";
    public final static String FIELD_CONTENT_TYPE="content_type";
    public final static String FIELD_SIZE="size";
    public final static String FIELD_SINGLE="single";
    public final static String FIELD_FORMAT="format";
    public final static String FIELD_FOLDER_ID = "folder_id";
    public final static String FIELD_DOUBLE = "double";
    public final static String FIELD_DATE = "date";

    public static class FormatConverter extends EnumConverter<FormatType> {

        public FormatConverter() {
            super(FormatType.class);
        }
    }

    public StoreFileReadable(long id) {
        super(id);
    }


    public String getFileName() throws DatabaseException {
        return getString(FIELD_FILE_NAME);
    }

    public String getContentType() throws DatabaseException {
        return getString(FIELD_CONTENT_TYPE);
    }

    public long getSize() throws DatabaseException {
        return getLong(FIELD_SIZE);
    }

    public boolean isSingle() throws DatabaseException {
        return getBoolean(FIELD_SINGLE);
    }

    public FormatType getFormat() throws DatabaseException {
        return get(FormatType.class, FIELD_FORMAT);
    }

    public Long getFolderId() throws DatabaseException {
        return getLong(FIELD_FOLDER_ID);
    }

    public Long getDouble() throws DatabaseException {
        return getLong(FIELD_DOUBLE);
    }

    public Long getDate() throws DatabaseException {
        return getLong(FIELD_DATE);
    }
}
