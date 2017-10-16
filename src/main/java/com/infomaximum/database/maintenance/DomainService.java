package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.core.schema.EntityPrefixIndex;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.modifier.Modifier;
import com.infomaximum.database.datasource.modifier.ModifierSet;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.IndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.ForeignDependencyException;
import com.infomaximum.database.exeption.InconsistentDatabaseException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.utils.PrefixIndexUtils;

import java.util.*;
import java.util.stream.Collectors;

public class DomainService {

    @FunctionalInterface
    public interface ModifierCreator {

        void apply(final DomainObject obj, long transactionId, List<Modifier> destination) throws DatabaseException;
    }

    private final int MAX_BATCH_SIZE = 8192;

    private final DataSource dataSource;

    private boolean isCreationMode = false;

    private StructEntity domain;
    private boolean existsData = false;

    public DomainService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DomainService setCreationMode(boolean value) {
        this.isCreationMode = value;
        return this;
    }

    public DomainService setDomain(StructEntity value) {
        this.domain = value;
        return this;
    }

    public void execute() throws DatabaseException {
        final String dataColumnFamily = domain.getColumnFamily();

        if (!dataSource.containsSequence(dataColumnFamily)) {
            if (isCreationMode) {
                dataSource.createSequence(dataColumnFamily);
            } else {
                throw new InconsistentDatabaseException("Sequence " + dataColumnFamily + " not found.");
            }
        }

        if (dataSource.containsColumnFamily(dataColumnFamily)) {
            existsData = existsKeys(dataColumnFamily);
        } else if (isCreationMode) {
            dataSource.createColumnFamily(dataColumnFamily);
            existsData = false;
        } else {
            throw new InconsistentDatabaseException("Domain " + dataColumnFamily + " not found.");
        }

        for (EntityIndex index : domain.getIndexes()) {
            ensureIndex(index);
        }

        for (EntityPrefixIndex index : domain.getPrefixIndexes()) {
            ensureIndex(index);
        }

        validateUnknownColumnFamilies();
        validateIntegrity();
    }

    static void removeDomainColumnFamiliesFrom(Set<String> columnFamilies, final StructEntity domain) {
        columnFamilies.remove(domain.getColumnFamily());

        for (EntityIndex index : domain.getIndexes()) {
            columnFamilies.remove(index.columnFamily);
        }

        for (EntityPrefixIndex index : domain.getPrefixIndexes()) {
            columnFamilies.remove(index.columnFamily);
        }
    }

    private void ensureIndex(EntityIndex index) throws DatabaseException {
        final String columnFamily = index.columnFamily;
        final boolean existsIndexedValues = ensureIndexColumnFamily(columnFamily);

        if (existsData) {
            if (existsIndexedValues || !isCreationMode) {
                return;
            }

            final Set<String> indexingFields = index.sortedFields.stream().map(EntityField::getName).collect(Collectors.toSet());
            final IndexKey indexKey = new IndexKey(0, new long[index.sortedFields.size()]);

            indexData(indexingFields, (obj, transactionId, destination) -> {
                indexKey.setId(obj.getId());
                IndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

                destination.add(new ModifierSet(index.columnFamily, indexKey.pack()));
            });
        } else if (existsIndexedValues) {
            throw new InconsistentDatabaseException("Index " + columnFamily + " is not empty, but " + domain.getColumnFamily() + " is empty.");
        }
    }

    private void ensureIndex(EntityPrefixIndex index) throws DatabaseException {
        final String columnFamily = index.columnFamily;
        final boolean existsIndexedValues = ensureIndexColumnFamily(columnFamily);

        if (existsData) {
            if (existsIndexedValues || !isCreationMode) {
                return;
            }

            final Set<String> indexingFields = Collections.singleton(index.field.getName());

            indexData(indexingFields, (obj, transactionId, destination) -> {
                Collection<String> lexemes = PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(String.class, index.field.getName()));
                PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, destination, dataSource, transactionId);
            });
        } else if (existsIndexedValues) {
            throw new InconsistentDatabaseException("Index " + columnFamily + " is not empty, but " + domain.getColumnFamily() + " is empty.");
        }
    }

    private void validateUnknownColumnFamilies() throws InconsistentDatabaseException {
        final String namespacePrefix = domain.getColumnFamily() + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> columnFamilies = Arrays.stream(dataSource.getColumnFamilies())
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());

        removeDomainColumnFamiliesFrom(columnFamilies, domain);

        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException(domain.getObjectClass() + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }

    private boolean ensureIndexColumnFamily(String columnFamily) throws DatabaseException {
        if (dataSource.containsColumnFamily(columnFamily)) {
            return existsKeys(columnFamily);
        }

        if (isCreationMode) {
            dataSource.createColumnFamily(columnFamily);
            return false;
        }

        throw new InconsistentDatabaseException("Index " + columnFamily + " not found.");
    }

    private void indexData(Set<String> loadingFields, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dataSource);
        long transactionId = dataSource.beginTransaction();
        try (IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(domain.getObjectClass(), EmptyFilter.INSTANCE, loadingFields)) {
            final List<Modifier> modifiers = new ArrayList<>();

            while (iter.hasNext()) {
                recordCreator.apply(iter.next(), transactionId, modifiers);

                if (modifiers.size() > MAX_BATCH_SIZE) {
                    dataSource.modify(modifiers, transactionId);
                    modifiers.clear();
                }
            }

            dataSource.modify(modifiers, transactionId);
            dataSource.commitTransaction(transactionId);
        } catch (Throwable e) {
            dataSource.rollbackTransaction(transactionId);
            throw e;
        }
    }

    private boolean existsKeys(String columnFamily) throws DataSourceDatabaseException {
        long iteratorId = dataSource.createIterator(columnFamily, null);
        try {
            return dataSource.next(iteratorId) != null;
        } finally {
            dataSource.closeIterator(iteratorId);
        }
    }

    private void validateIntegrity() throws DatabaseException {
        if (!existsData) {
            return;
        }

        List<EntityField> foreignFields = domain.getFields()
                .stream()
                .filter(EntityField::isForeign)
                .collect(Collectors.toList());

        if (foreignFields.isEmpty()) {
            return;
        }

        Set<String> fieldNames = foreignFields
                .stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());

        FieldKey fieldKey = new FieldKey(0);

        DomainObjectSource domainObjectSource = new DomainObjectSource(dataSource);
        try (IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(domain.getObjectClass(), EmptyFilter.INSTANCE, fieldNames)) {
            while (iter.hasNext()) {
                DomainObject obj = iter.next();

                for (EntityField field : foreignFields) {
                    Long value = obj.get(Long.class, field.getName());
                    if (value == null) {
                        continue;
                    }

                    fieldKey.setId(value);
                    if (dataSource.getValue(field.getForeignDependency().getColumnFamily(), fieldKey.pack()) == null) {
                        throw new ForeignDependencyException(obj.getId(), domain.getObjectClass(), field, value);
                    }
                }
            }
        }
    }
}
