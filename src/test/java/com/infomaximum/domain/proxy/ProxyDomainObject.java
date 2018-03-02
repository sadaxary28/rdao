package com.infomaximum.domain.proxy;

import com.infomaximum.database.domainobject.DomainObject;

import com.infomaximum.database.exception.DatabaseException;

import java.util.Date;

public class ProxyDomainObject extends DomainObject {

    public ProxyDomainObject(long id) {
        super(id);
    }

    @Override
    public <T> T get(Class<T> type, String fieldName) {
        try {
            return super.get(type, fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getString(String fieldName) {
        try {
            return super.getString(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Integer getInteger(String fieldName) {
        try {
            return super.getInteger(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Long getLong(String fieldName) {
        try {
            return super.getLong(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Date getDate(String fieldName) {
        try {
            return super.getDate(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Boolean getBoolean(String fieldName) {
        try {
            return super.getBoolean(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected byte[] getBytes(String fieldName) {
        try {
            return super.getBytes(fieldName);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }
    }
}
