package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;

import java.util.*;
import java.util.stream.Collectors;

/*
 Не потоко безопасный класс
 */
public class DatabaseService {

    private final DataSource dataSource;

    private boolean isCreationMode = false;

    private String namespace;
    private Set<StructEntity> domains = new HashSet<>();

    private String namespacePrefix;

    public DatabaseService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DatabaseService setCreationMode(boolean value) {
        this.isCreationMode = value;
        return this;
    }

    public DatabaseService setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespacePrefix = namespace + StructEntity.NAMESPACE_SEPARATOR;
        return this;
    }

    public DatabaseService withDomain(Class<? extends DomainObject> clazz) {
        if (!domains.add(StructEntity.getInstance(clazz))) {
            throw new RuntimeException("Class " + clazz + " or same class already exists.");
        }
        return this;
    }

    public void execute() throws DatabaseException {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException();
        }

        validateConsistentNames();

        DomainService domainService = new DomainService(dataSource)
                .setCreationMode(isCreationMode);
        for (StructEntity domain : domains) {
            domainService.execute(domain);
        }

        validateUnknownColumnFamilies();
    }

    private void validateConsistentNames() throws InconsistentDatabaseException {
        Set<String> processedNames = new HashSet<>();
        for (StructEntity domain : domains) {
            if (processedNames.contains(domain.getColumnFamily())) {
                throw new InconsistentDatabaseException("Column family " + domain.getColumnFamily() + " into " + domain.getObjectClass() + " already exists.");
            }

            if (!domain.getColumnFamily().startsWith(namespacePrefix)) {
                throw new InconsistentDatabaseException("Namespace " + namespace + " is not consistent with " + domain.getObjectClass());
            }

            processedNames.add(domain.getColumnFamily());
        }
    }

    private void validateUnknownColumnFamilies() throws InconsistentDatabaseException {
        Set<String> columnFamilies = Arrays.asList(dataSource.getColumnFamilies())
                .stream()
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());
        for (StructEntity domain : domains) {
            DomainService.removeDomainColumnFamiliesFrom(columnFamilies, domain);
        }

        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException("Namespace " + namespace + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }
}
