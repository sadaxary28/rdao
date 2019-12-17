package com.infomaximum.database.maintenance;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.exception.InconsistentDatabaseException;
import com.infomaximum.database.exception.IndexAlreadyExistsException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.schema.newschema.*;
import com.infomaximum.database.schema.newschema.Schema;
import com.infomaximum.database.utils.HashIndexUtils;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.RangeIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.utils.key.HashIndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;
import com.infomaximum.database.utils.key.RangeIndexKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class DomainService {

    private final static Logger log = LoggerFactory.getLogger(DomainService.class);

    @FunctionalInterface
    private interface ModifierCreator {

        void apply(final DomainObject obj, DBTransaction transaction) throws DatabaseException;
    }

    private final DBProvider dbProvider;

    private ChangeMode changeMode = ChangeMode.NONE;
    private boolean isValidationMode = false;

    private StructEntity domain;
    private final Schema dbSchema;
    private boolean existsData = false;

    public DomainService(DBProvider dbProvider, Schema dbSchema) {
        this.dbProvider = dbProvider;
        this.dbSchema = dbSchema;
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

        if (!dbProvider.containsSequence(dataColumnFamily)) {
            if (changeMode == ChangeMode.CREATION) {
                dbProvider.createSequence(dataColumnFamily);
            } else if (isValidationMode) {
                throw new InconsistentDatabaseException("Sequence " + dataColumnFamily + " not found.");
            }
        }

        existsData = ensureColumnFamily(dataColumnFamily);
        ensureIndexes();

        if (changeMode == ChangeMode.REMOVAL) {
            remove();
        }

        validate();
    }

    static void removeDomainColumnFamiliesFrom(Set<String> columnFamilies, final StructEntity domain) {
        columnFamilies.remove(domain.getColumnFamily());
        columnFamilies.remove(domain.getIndexColumnFamily());
    }

    private void remove() throws DatabaseException {
        dbSchema.dropTable(domain.getName(), domain.getNamespace());
        for (String columnFamily : getColumnFamilies()) {
            dbProvider.dropColumnFamily(columnFamily);
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

    private void doIndex(HashIndex index) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
        try {
            dbSchema.createIndex(index, domain.getName(), domain.getNamespace());
        } catch (IndexAlreadyExistsException e) {
            log.warn("Index already exists", e);
        }
    }

    private void doPrefixIndex(PrefixIndex index) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, (obj, transaction) -> {
            lexemes.clear();
            for (Field field : index.sortedFields) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getNumber()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, transaction);
        });
        try {
            dbSchema.createIndex(index, domain.getName(), domain.getNamespace());
        } catch (IndexAlreadyExistsException e) {
            log.warn("Index already exists", e);
        }
    }

    private void doIntervalIndex(IntervalIndex index) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final List<Field> hashedFields = index.getHashedFields();
        final Field indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getNumber()));

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
        try {
            dbSchema.createIndex(index, domain.getName(), domain.getNamespace());
        } catch (IndexAlreadyExistsException e) {
            log.warn("Index already exists", e);
        }
    }

    private void doIntervalIndex(RangeIndex index) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final List<Field> hashedFields = index.getHashedFields();
        final RangeIndexKey indexKey = new RangeIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            RangeIndexUtils.insertIndexedRange(index, indexKey,
                    obj.get(index.getBeginIndexedField().getNumber()),
                    obj.get(index.getEndIndexedField().getNumber()),
                    transaction);
        });
        try {
            dbSchema.createIndex(index, domain.getName(), domain.getNamespace());
        } catch (IndexAlreadyExistsException e) {
            log.warn("Index already exists", e);
        }
    }

    private Set<String> getColumnFamilies() throws DatabaseException {
        final String namespacePrefix = domain.getColumnFamily() + StructEntity.NAMESPACE_SEPARATOR;
        Set<String> result = Arrays.stream(dbProvider.getColumnFamilies())
                .filter(s -> s.startsWith(namespacePrefix))
                .collect(Collectors.toSet());
        result.add(domain.getColumnFamily());
        return result;
    }

    private void validateUnknownColumnFamilies() throws DatabaseException {
        Set<String> columnFamilies = getColumnFamilies();
        removeDomainColumnFamiliesFrom(columnFamilies, domain);
        if (!columnFamilies.isEmpty()) {
            throw new InconsistentDatabaseException(domain.getObjectClass() + " contains unknown column families " + String.join(", ", columnFamilies) + ".");
        }
    }

    private boolean ensureColumnFamily(String columnFamily) throws DatabaseException {
        if (dbProvider.containsColumnFamily(columnFamily)) {
            return existsKeys(columnFamily);
        }

        if (changeMode == ChangeMode.CREATION) {
            dbProvider.createColumnFamily(columnFamily);
        } else if (isValidationMode) {
            throw new InconsistentDatabaseException("Column family " + columnFamily + " not found.");
        }
        return false;
    }

    private void ensureIndexes() throws DatabaseException {
        final boolean existsIndexedValues = ensureColumnFamily(domain.getIndexColumnFamily());

        if (existsData) {
            if (changeMode != ChangeMode.CREATION) {
                return;
            }
            try (DBIterator i = dbProvider.createIterator(domain.getIndexColumnFamily())) {
                //HashIndex
                for (HashIndex index : domain.getHashIndexes()) {
                    if (i.seek(new KeyPattern(index.attendant)) == null) {
                        doIndex(index);
                    }
                }
                //PrefixIndex
                for (PrefixIndex index : domain.getPrefixIndexes()) {
                    if (i.seek(new KeyPattern(index.attendant)) == null) {
                        doPrefixIndex(index);
                    }
                }
                //IntervalIndex
                for (IntervalIndex index : domain.getIntervalIndexes()) {
                    if (i.seek(new KeyPattern(index.attendant)) == null) {
                        doIntervalIndex(index);
                    }
                }
                //RangeIndex
                for (RangeIndex index : domain.getRangeIndexes()) {
                    if (i.seek(new KeyPattern(index.attendant)) == null) {
                        doIntervalIndex(index);
                    }
                }
            }
        } else if (existsIndexedValues && isValidationMode) {
            throw new InconsistentDatabaseException(domain.getIndexColumnFamily() + " is not empty, but " + domain.getColumnFamily() + " is empty.");
        }
    }

    private void indexData(Set<Integer> loadingFields, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(domain.getObjectClass(), EmptyFilter.INSTANCE, loadingFields)) {
            while (iter.hasNext()) {
                recordCreator.apply(iter.next(), transaction);
            }

            transaction.commit();
        }
    }

    private boolean existsKeys(String columnFamily) throws DatabaseException {
        try (DBIterator i = dbProvider.createIterator(columnFamily)) {
            return i.seek(null) != null;
        }
    }

    private void validateIntegrity() throws DatabaseException {
        if (!existsData) {
            return;
        }

        List<Field> foreignFields = Arrays.stream(domain.getFields())
                .filter(Field::isForeign)
                .collect(Collectors.toList());

        if (foreignFields.isEmpty()) {
            return;
        }

        Set<Integer> fieldNames = foreignFields
                .stream()
                .map(Field::getNumber)
                .collect(Collectors.toSet());

        FieldKey fieldKey = new FieldKey(0);

        RangeSet<Long>[] processedIds = new RangeSet[domain.getFields().length];
        for (int i = 0; i < foreignFields.size(); ++i) {
            Field field = foreignFields.get(i);
            processedIds[field.getNumber()] = TreeRangeSet.create();
        }

        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(domain.getObjectClass(), EmptyFilter.INSTANCE, fieldNames)) {
            while (iter.hasNext()) {
                DomainObject obj = iter.next();

                for (int i = 0; i < foreignFields.size(); ++i) {
                    Field field = foreignFields.get(i);
                    Long value = obj.get(field.getNumber());
                    if (value == null) {
                        continue;
                    }

                    RangeSet<Long> processedId = processedIds[field.getNumber()];
                    if (processedId.contains(value)) {
                        continue;
                    }

                    fieldKey.setId(value);
                    if (dbProvider.getValue(field.getForeignDependency().getColumnFamily(), fieldKey.pack()) == null) {
                        throw new ForeignDependencyException(obj.getId(), domain.getObjectClass(), field, value);
                    }
                    processedId.add(Range.closedOpen(value, value + 1));
                }
            }
        }
    }
}
