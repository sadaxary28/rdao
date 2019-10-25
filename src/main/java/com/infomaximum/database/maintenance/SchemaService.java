package com.infomaximum.database.maintenance;

import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.InconsistentDatabaseException;

import java.util.*;
import java.util.stream.Collectors;

/*
 Не потоко безопасный класс
 */
public class SchemaService {

    private final DBProvider dbProvider;

    private ChangeMode changeModeMode = ChangeMode.NONE;
    private boolean isValidationMode = false;
    private String namespace;
    private Schema schema;
    private Set<String> ignoringNamespaces = new HashSet<>();

    public SchemaService(DBProvider dbProvider) {
        this.dbProvider = dbProvider;
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
            new DomainService(dbProvider, schema.getDbSchema())
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

    private void validateUnknownColumnFamilies() throws DatabaseException {
        final String namespacePrefix = namespace + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> columnFamilies = Arrays.stream(dbProvider.getColumnFamilies())
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
