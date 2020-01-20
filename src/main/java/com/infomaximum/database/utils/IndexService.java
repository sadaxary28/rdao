package com.infomaximum.database.utils;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.utils.key.HashIndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;

import java.util.*;
import java.util.stream.Collectors;

public class IndexService {

    @FunctionalInterface
    private interface ModifierCreator {
        void apply(final DomainObject obj, DBTransaction transaction) throws DatabaseException;
    }

    //todo check existed indexes and throw exception
    public static void doIndex(HashIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doIndex(DBHashIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(IndexUtils.getFieldsByIds(table.getSortedFields(), index.getFieldIds()),
                    obj,
                    indexKey.getFieldValues());

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doPrefixIndex(PrefixIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            lexemes.clear();
            for (Field field : index.sortedFields) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getNumber()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, transaction);
        });
    }

    public static void doPrefixIndex(DBPrefixIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            lexemes.clear();
            for (DBField field : IndexUtils.getFieldsByIds(table.getSortedFields(), index.getFieldIds())) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getId()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, table.getIndexColumnFamily(), transaction);
        });
    }

    public static void doIntervalIndex(IntervalIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final List<Field> hashedFields = index.getHashedFields();
        final Field indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getNumber()));

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doIntervalIndex(DBIntervalIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = Arrays.stream(index.getFieldIds()).boxed().collect(Collectors.toSet());
        final DBField[] hashedFields = IndexUtils.getFieldsByIds(table.getSortedFields(), index.getHashFieldIds());
        final DBField indexedField = IndexUtils.getFieldsByIds(table.getSortedFields(), index.getIndexedFieldId());
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.length], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getId()));

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    private static void indexData(Set<Integer> loadingFields, StructEntity table, DBProvider dbProvider, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(table.getObjectClass(), EmptyFilter.INSTANCE, loadingFields)) {
             while (iter.hasNext()) {
                 recordCreator.apply(iter.next(), transaction);
             }
            transaction.commit();
        }
    }

    private static void indexData(Set<Integer> loadingFields, DBTable table, DBProvider dbProvider, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(Schema.getTableClass(table.getName(), table.getNamespace()),
                     EmptyFilter.INSTANCE,
                     loadingFields)) {
             while (iter.hasNext()) {
                 recordCreator.apply(iter.next(), transaction);
             }
            transaction.commit();
        }
    }
}
