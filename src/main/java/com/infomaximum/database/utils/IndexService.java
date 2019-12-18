package com.infomaximum.database.utils;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.IndexAlreadyExistsException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.newschema.*;
import com.infomaximum.database.utils.key.HashIndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;

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
    public static void doIndex(HashIndex index, StructEntity table, DBProvider dbProvider) throws DatabaseException {
        final Set<Integer> indexingFields = index.sortedFields.stream().map(Field::getNumber).collect(Collectors.toSet());
        final HashIndexKey indexKey = new HashIndexKey(0, index);

        indexData(indexingFields, table, dbProvider, (obj, transaction) -> {
            indexKey.setId(obj.getId());
            HashIndexUtils.setHashValues(index.sortedFields, obj, indexKey.getFieldValues());

            transaction.put(index.columnFamily, indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
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
}
