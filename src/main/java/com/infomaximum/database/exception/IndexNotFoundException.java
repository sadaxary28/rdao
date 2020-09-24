package com.infomaximum.database.exception;

import com.infomaximum.database.domainobject.DomainObject;

public class IndexNotFoundException extends RuntimeException {

    public IndexNotFoundException(String indexName, Class<? extends DomainObject> domainClass) {
        super("Not found " + indexName + " in " + domainClass);
    }

}

