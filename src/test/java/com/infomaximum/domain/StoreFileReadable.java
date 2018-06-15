package com.infomaximum.domain;

import com.infomaximum.database.anotation.*;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.EnumConverter;
import com.infomaximum.domain.type.FormatType;

import java.time.Instant;

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
                @Field(name = StoreFileReadable.FIELD_DATE, type = Instant.class),
                @Field(name = StoreFileReadable.FIELD_BEGIN, type = Long.class),
                @Field(name = StoreFileReadable.FIELD_END, type = Long.class)
        },
        hashIndexes = {
                @HashIndex(fields = {StoreFileReadable.FIELD_SIZE}),
                @HashIndex(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @HashIndex(fields = {StoreFileReadable.FIELD_SIZE, StoreFileReadable.FIELD_FILE_NAME}),
                @HashIndex(fields = {StoreFileReadable.FIELD_FORMAT})
        },
        prefixIndexes = {
                @PrefixIndex(fields = {StoreFileReadable.FIELD_FILE_NAME}),
                @PrefixIndex(fields = {StoreFileReadable.FIELD_FILE_NAME, StoreFileReadable.FIELD_CONTENT_TYPE})
        },
        intervalIndexes = {
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_DOUBLE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_DATE),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FILE_NAME}),
                @IntervalIndex(indexedField = StoreFileReadable.FIELD_SIZE, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID})
        },
        rangeIndexes = {
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN, endField = StoreFileReadable.FIELD_END),
                @RangeIndex(beginField = StoreFileReadable.FIELD_BEGIN, endField = StoreFileReadable.FIELD_END, hashedFields = {StoreFileReadable.FIELD_FOLDER_ID})
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

    public final static String FIELD_BEGIN = "begin";
    public final static String FIELD_END = "end";

    public final static RangeFilter.IndexedField RANGE_INDEXED_FIELD = new RangeFilter.IndexedField(FIELD_BEGIN, FIELD_END);

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
        return get(FIELD_FORMAT);
    }

    public Long getFolderId() throws DatabaseException {
        return getLong(FIELD_FOLDER_ID);
    }

    public Double getDouble() throws DatabaseException {
        return get(FIELD_DOUBLE);
    }

    public Instant getInstant() throws DatabaseException {
        return getInstant(FIELD_DATE);
    }

    public Long getBegin() {
        return getLong(FIELD_BEGIN);
    }
}
