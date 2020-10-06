package com.infomaximum.database.exception;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBTable;

import java.util.Set;
import java.util.stream.Collectors;

public class IndexNotFoundException extends DatabaseException {

    public IndexNotFoundException(String indexName, Class<? extends DomainObject> domainClass) {
        super("Not found " + indexName + " in " + domainClass);
    }

    public IndexNotFoundException(Set<Integer> indexedFieldIds, DBTable table) {
        super("Not found " +
                indexedFieldIds.stream()
                        .map(table::getField)
                        .map(DBField::getName)
                        .collect(Collectors.joining(", ")) +
                " in " + table.getNamespace() + "." + table.getName());
    }

}

