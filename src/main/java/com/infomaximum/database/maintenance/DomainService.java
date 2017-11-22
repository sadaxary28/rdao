package com.infomaximum.database.maintenance;

import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.schema.*;
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
    private interface ModifierCreator {

        void apply(final DomainObject obj, long transactionId, List<Modifier> destination) throws DatabaseException;
    }

    @FunctionalInterface
    private interface IndexAction {
        void apply() throws DatabaseException;
    }

    private final int MAX_BATCH_SIZE = 8192;

    private final DataSource dataSource;

    private ChangeMode changeMode = ChangeMode.NONE;
    private boolean isValidationMode = false;

    private StructEntity domain;
    private boolean existsData = false;

    public DomainService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DomainService setChangeMode(ChangeMode value) {
        this.changeMode = value;
        return this;
    }

    public DomainService setValidationMode(boolean value) {
        this.isValidationMode = value;
        return this;
    }

    public DomainService setDomain(StructEntity value) {
        this.domain = value;
        return this;
    }

    public void execute() throws DatabaseException {
        final String dataColumnFamily = domain.getColumnFamily();

        if (!dataSource.containsSequence(dataColumnFamily)) {
            if (changeMode == ChangeMode.CREATION) {
                dataSource.createSequence(dataColumnFamily);
            } else if (isValidationMode) {
                throw new InconsistentDatabaseException("Sequence " + dataColumnFamily + " not found.");
            }
        }

        existsData = ensureColumnFamily(dataColumnFamily);

        for (EntityIndex index : domain.getIndexes()) {
            ensureIndex(index, () -> doIndex(index));
        }

        for (EntityPrefixIndex index : domain.getPrefixIndexes()) {
            ensureIndex(index, () -> doPrefixIndex(index));
        }

        if (changeMode == ChangeMode.REMOVAL) {
            remove();
        }

        validate();
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

    private void remove() throws DatabaseException {
        for (String columnFamily : getColumnFamilies()) {
            dataSource.dropColumnFamily(columnFamily);
        }
    }

    private void validate() throws DatabaseException {
        if (!isValidationMode) {
            return;
        }

        validateUnknownColumnFamilies();

        if (changeMode != ChangeMode.REMOVAL) {
            validateIntegrity();
        }
    }

    private <T extends BaseIndex> void ensureIndex(T index, IndexAction indexAction) throws DatabaseException {
        final boolean existsIndexedValues = ensureColumnFamily(index.columnFamily);

        if (existsData) {
            if (existsIndexedValues || changeMode != ChangeMode.CREATION) {
                return;
            }

            indexAction.apply();
        } else if (existsIndexedValues && isValidationMode) {
            throw new InconsistentDatabaseException("Index " + index.columnFamily + " is not empty, but " + domain.getColumnFamily() + " is empty.");
        }
    }

    private void doIndex(EntityIndex index) throws DatabaseException {
        final Set<String> indexingFields = index.sortedFields.stream().map(EntityField::getName).collect(Collectors.toSet());
        final IndexKey indexKey = new IndexKey(0, new long[index.sortedFields.size()]);

        indexData(indexingFields, (obj, transactionId, destination) -> {
            indexKey.setId(obj.getId());
            IndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

            destination.add(new ModifierSet(index.columnFamily, indexKey.pack()));
        });
    }

    private void doPrefixIndex(EntityPrefixIndex index) throws DatabaseException {
        final Set<String> indexingFields = index.sortedFields.stream().map(EntityField::getName).collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, (obj, transactionId, destination) -> {
            lexemes.clear();
            for (EntityField field : index.sortedFields) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(String.class, field.getName()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, destination, dataSource, transactionId);
        });
    }

    private Set<String> getColumnFamilies() {
        final String namespacePrefix = domain.getColumnFamily() + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> result = Arrays.stream(dataSource.getColumnFamilies())
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());
        result.add(domain.getColumnFamily());
        return result;
    }

    private void validateUnknownColumnFamilies() throws InconsistentDatabaseException {
        Set<String> columnFamilies = getColumnFamilies();
        removeDomainColumnFamiliesFrom(columnFamilies, domain);
        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException(domain.getObjectClass() + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }

    private boolean ensureColumnFamily(String columnFamily) throws DatabaseException {
        if (dataSource.containsColumnFamily(columnFamily)) {
            return existsKeys(columnFamily);
        }

        if (changeMode == ChangeMode.CREATION) {
            dataSource.createColumnFamily(columnFamily);
        } else if (isValidationMode) {
            throw new InconsistentDatabaseException("Column family " + columnFamily + " not found.");
        }
        return false;
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
        long iteratorId = dataSource.createIterator(columnFamily);
        try {
            return dataSource.seek(iteratorId, null) != null;
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
