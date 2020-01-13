package com.infomaximum.database.utils;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.utils.key.HashIndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;
import com.infomaximum.database.utils.key.RangeIndexKey;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class IndexService {

    @FunctionalInterface
    private interface ModifierCreator {
        void apply(final DomainObject obj, DBTransaction transaction) throws DatabaseException;
    }

    //todo check existed indexes and throw exception
    public static void doIndex(DBHashIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.getFields().stream().map(DBObject::getId).collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(index.getFields(), obj, indexKey.getFieldValues());

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doPrefixIndex(DBPrefixIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.getFields().stream().map(DBObject::getId).collect(Collectors.toSet());
        final SortedSet<String> lexemes = PrefixIndexUtils.buildSortedSet();

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            lexemes.clear();
            for (DBField field : index.getFields()) {
                PrefixIndexUtils.splitIndexingTextIntoLexemes(obj.get(field.getId()), lexemes);
            }
            PrefixIndexUtils.insertIndexedLexemes(index, obj.getId(), lexemes, transaction);
        });
    }

    public static void doIntervalIndex(DBIntervalIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.getFields().stream().map(DBObject::getId).collect(Collectors.toSet());
        final List<DBField> hashedFields = index.getHashFields();
        final DBField indexedField = index.getIndexedField();
        final IntervalIndexKey indexKey = new IntervalIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            indexKey.setIndexedValue(obj.get(indexedField.getId()));

            transaction.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        });
    }

    public static void doIntervalIndex(DBRangeIndex index, DBTable table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.getFields().stream().map(DBField::getId).collect(Collectors.toSet());
        final List<DBField> hashedFields = index.getHashFields();
        final RangeIndexKey indexKey = new RangeIndexKey(0, new long[hashedFields.size()], index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(hashedFields, obj, indexKey.getHashedValues());
            RangeIndexUtils.insertIndexedRange(index, indexKey,
                    obj.get(index.getBeginField().getId()),
                    obj.get(index.getEndField().getId()),
                    transaction);
        });
    }

    private static void indexData(Set<Integer> loadingFields, DBTable table, DBProvider dbProvider, ModifierCreator recordCreator) throws DatabaseException {
        DomainObjectSource domainObjectSource = new DomainObjectSource(dbProvider);
        try (DBTransaction transaction = dbProvider.beginTransaction();
             IteratorEntity<? extends DomainObject> iter = domainObjectSource.find(table.getObjectClass(), EmptyFilter.INSTANCE, loadingFields)) {
             while (iter.hasNext()) {
                 recordCreator.apply(iter.next(), transaction);
             }
            transaction.commit();
        }
    }
}
