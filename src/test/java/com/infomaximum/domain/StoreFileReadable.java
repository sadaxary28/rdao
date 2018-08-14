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
                @Field(number = StoreFileReadable.FIELD_FILE_NAME, name = "name", type = String.class),
                @Field(number = StoreFileReadable.FIELD_CONTENT_TYPE, name = "type", type = String.class),
                @Field(number = StoreFileReadable.FIELD_SIZE, name = "size", type = Long.class),
                @Field(number = StoreFileReadable.FIELD_SINGLE, name = "single", type = Boolean.class),
                @Field(number = StoreFileReadable.FIELD_FORMAT, name = "format", type = FormatType.class, packerType = StoreFileReadable.FormatConverter.class),
                @Field(number = StoreFileReadable.FIELD_FOLDER_ID, name = "folder_id", type = Long.class, foreignDependency = ExchangeFolderReadable.class),
                @Field(number = StoreFileReadable.FIELD_DOUBLE, name = "double", type = Double.class),
                @Field(number = StoreFileReadable.FIELD_DATE, name = "date", type = Instant.class),
                @Field(number = StoreFileReadable.FIELD_BEGIN, name = "begin", type = Long.class),
                @Field(number = StoreFileReadable.FIELD_END, name = "end", type = Long.class)
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

    public final static int FIELD_FILE_NAME=0;
    public final static int FIELD_CONTENT_TYPE=1;
    public final static int FIELD_SIZE=2;
    public final static int FIELD_SINGLE=3;
    public final static int FIELD_FORMAT=4;
    public final static int FIELD_FOLDER_ID = 5;
    public final static int FIELD_DOUBLE = 6;
    public final static int FIELD_DATE = 7;

    public final static int FIELD_BEGIN = 8;
    public final static int FIELD_END = 9;

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

    public Long getEnd() {
        return getLong(FIELD_END);
    }
}
