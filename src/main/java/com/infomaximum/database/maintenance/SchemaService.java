package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;

import java.util.*;
import java.util.stream.Collectors;

/*
 Не потоко безопасный класс
 */
public class SchemaService {

    private final DataSource dataSource;

    private ChangeMode changeModeMode = ChangeMode.NONE;
    private boolean isValidationMode = false;
    private String namespace;
    private Schema schema;
    private Set<String> ignoringNamespaces = new HashSet<>();

    public SchemaService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public SchemaService setChangeMode(ChangeMode value) {
        this.changeModeMode = value;
        return this;
    }

    public SchemaService setValidationMode(boolean value) {
        this.isValidationMode = value;
        return this;
    }

    public SchemaService setNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public SchemaService appendIgnoringNamespace(String namespace) {
        ignoringNamespaces.add(namespace);
        return this;
    }

    public SchemaService setSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    public void execute() throws DatabaseException {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException();
        }

        validateConsistentNames();

        for (StructEntity domain : schema.getDomains()) {
            new DomainService(dataSource)
                    .setChangeMode(changeModeMode)
                    .setValidationMode(isValidationMode)
                    .setDomain(domain)
                    .execute();
        }

        validate();
    }

    private void validate() throws DatabaseException {
        if (isValidationMode) {
            validateUnknownColumnFamilies();
        }
    }

    private void validateConsistentNames() throws InconsistentDatabaseException {
        final String namespacePrefix = namespace + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> processedNames = new HashSet<>();
        for (StructEntity domain : schema.getDomains()) {
            if (processedNames.contains(domain.getColumnFamily())) {
                throw new InconsistentDatabaseException("Column family " + domain.getColumnFamily() + " into " + domain.getObjectClass() + " already exists.");
            }

            if (!domain.getColumnFamily().startsWith(namespacePrefix)) {
                throw new InconsistentDatabaseException("Namespace " + namespace + " is not consistent with " + domain.getObjectClass());
            }

            processedNames.add(domain.getColumnFamily());
        }

        for (String value : ignoringNamespaces) {
            if (!value.startsWith(namespacePrefix)) {
                throw new InconsistentDatabaseException("Namespace " + namespace + " is not consistent with " + value);
            }
        }
    }

    private void validateUnknownColumnFamilies() throws InconsistentDatabaseException {
        final String namespacePrefix = namespace + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> columnFamilies = Arrays.stream(dataSource.getColumnFamilies())
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());
        for (StructEntity domain : schema.getDomains()) {
            DomainService.removeDomainColumnFamiliesFrom(columnFamilies, domain);
        }

        for (String space : ignoringNamespaces) {
            final String spacePrefix = space + StructEntity.NAMESPACE_SEPARATOR;
            columnFamilies.removeIf(s -> s.startsWith(spacePrefix));
        }

        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException("Namespace " + namespace + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }
}
