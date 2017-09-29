package com.infomaximum.rocksdb.domain.proxy;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.BaseEnum;

import java.util.Date;

public class ProxyDomainObject extends DomainObject {

    public ProxyDomainObject(long id) {
        super(id);
    }

    @Override
    public <T> T get(Class<T> type, String fieldName) {
        try {
            return super.get(type, fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getString(String fieldName) {
        try {
            return super.getString(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Integer getInteger(String fieldName) {
        try {
            return super.getInteger(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Long getLong(String fieldName) {
        try {
            return super.getLong(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Date getDate(String fieldName) {
        try {
            return super.getDate(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Boolean getBoolean(String fieldName) {
        try {
            return super.getBoolean(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected byte[] getBytes(String fieldName) {
        try {
            return super.getBytes(fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected <T extends Enum & BaseEnum> T getEnum(Class<T> enumClass, String fieldName) {
        try {
            return super.getEnum(enumClass, fieldName);
        } catch (DataSourceDatabaseException e) {
            throw new RuntimeException(e);
        }
    }
}
