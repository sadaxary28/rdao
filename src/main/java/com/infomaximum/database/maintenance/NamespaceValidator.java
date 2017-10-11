package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;

import java.util.*;

public class NamespaceValidator {

    private final DataSource dataSource;

    private final Set<String> namespacePrefixes = new HashSet<>();

    public NamespaceValidator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public NamespaceValidator withNamespace(String namespace) {
        if (!namespacePrefixes.add(namespace + StructEntity.NAMESPACE_SEPARATOR)) {
            throw new RuntimeException("Namespace " + namespace + " already exists.");
        }
        return this;
    }

    public void execute() throws DatabaseException {
        validateUnknownColumnFamilies();
    }

    private void validateUnknownColumnFamilies() throws InconsistentDatabaseException {
        List<String> columnFamilies = Arrays.asList(dataSource.getColumnFamilies());
        for (String columnFamily : columnFamilies) {
            if (!contains(columnFamily)) {
                throw new InconsistentDatabaseException("Unknown column family " + columnFamily + " .");
            }
        }
    }

    private boolean contains(String columnFamily) {
        for (String namespacePrefix : namespacePrefixes) {
            if (columnFamily.startsWith(namespacePrefix)) {
                return true;
            }
        }

        return false;
    }
}
