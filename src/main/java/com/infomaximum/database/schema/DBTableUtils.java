package com.infomaximum.database.schema;

import com.infomaximum.database.exception.FieldNotFoundException;
import com.infomaximum.database.exception.runtime.SchemaException;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.schema.table.THashIndex;
import com.infomaximum.database.schema.table.TIntervalIndex;
import com.infomaximum.database.schema.table.TPrefixIndex;
import com.infomaximum.database.schema.table.TRangeIndex;

import java.util.Arrays;
import java.util.Comparator;

class DBTableUtils {

//    static Table buildTable(DBTable table, DBSchema schema) throws SchemaException {
//        return new Table(
//                table.getName(),
//                table.getFields().stream().map(field -> DBTableUtils.buildField(field, schema)).collect(Collectors.toList()),
//                table.getHashIndexes().stream().map(index -> DBTableUtils.buildIndex(index, table)).collect(Collectors.toList()),
//                table.getPrefixIndexes().stream().map(index -> DBTableUtils.buildIndex(index, table)).collect(Collectors.toList()),
//                table.getIntervalIndexes().stream().map(index -> DBTableUtils.buildIndex(index, table)).collect(Collectors.toList()),
//                table.getRangeIndexes().stream().map(index -> DBTableUtils.buildIndex(index, table)).collect(Collectors.toList())
//        );
//    }

//    static Field buildField(DBField field, DBSchema schema) throws SchemaException {
//        if (field.isForeignKey()) {
//            DBTable foreignTable = schema.getTables().stream()
//                    .filter(table -> table.getId() == field.getForeignTableId())
//                    .findFirst()
//                    .orElseThrow(() -> new TableNotFoundException(field.getForeignTableId()));
//
//            return new Field(field.getName(), foreignTable.getName());
//        }
//        return new Field(field.getName(), field.getType());
//    }
//
//    static HashIndex buildIndex(DBHashIndex index, DBTable table) throws SchemaException {
//        return new HashIndex(toFieldNames(index.getFieldIds(), table));
//    }
//
//    static PrefixIndex buildIndex(DBPrefixIndex index, DBTable table) throws SchemaException {
//        return new PrefixIndex(toFieldNames(index.getFieldIds(), table));
//    }
//
//    static IntervalIndex buildIndex(DBIntervalIndex index, DBTable table) throws SchemaException {
//        return new IntervalIndex(
//                getFieldName(index.getIndexedFieldId(), table),
//                toFieldNames(index.getHashFieldIds(), table)
//        );
//    }
//
//    static RangeIndex buildIndex(DBRangeIndex index, DBTable table) throws SchemaException {
//        return new RangeIndex(
//                getFieldName(index.getBeginFieldId(), table),
//                getFieldName(index.getEndFieldId(), table),
//                toFieldNames(index.getHashFieldIds(), table)
//        );
//    }

    private static String getFieldName(int fieldId, DBTable table) throws SchemaException {
        return table.getSortedFields().stream()
                .filter(field -> field.getId() == fieldId)
                .findFirst()
                .orElseThrow(() -> new FieldNotFoundException(fieldId, table.getName()))
                .getName();
    }

    static DBHashIndex buildIndex(HashIndex index, DBTable table) throws SchemaException {
        return new DBHashIndex(toSortedFieldIds(index.getFieldNames(), table));
    }

    static DBHashIndex buildIndex(THashIndex index, DBTable table) throws SchemaException {
        return new DBHashIndex(toSortedFieldIds(index.getFields(), table));
    }

    static DBPrefixIndex buildIndex(PrefixIndex index, DBTable table) throws SchemaException {
        return new DBPrefixIndex(toSortedFieldIds(index.getFieldNames(), table));
    }

    static DBPrefixIndex buildIndex(TPrefixIndex index, DBTable table) throws SchemaException {
        return new DBPrefixIndex(toSortedFieldIds(index.getFields(), table));
    }

    static DBIntervalIndex buildIndex(IntervalIndex index, DBTable table) throws SchemaException {
        return new DBIntervalIndex(
                table.getField(index.getIndexedField().getName()),
                toSortedFieldIds(index.getHashedFields().stream().map(Field::getName).toArray(String[]::new), table)
        );
    }

    static DBIntervalIndex buildIndex(TIntervalIndex index, DBTable table) throws SchemaException {
        return new DBIntervalIndex(
                table.getField(index.getIndexedField()),
                toSortedFieldIds(index.getHashedFields(), table)
        );
    }

    static DBRangeIndex buildIndex(RangeIndex index, DBTable table) throws SchemaException {
        return new DBRangeIndex(
                table.getField(index.getBeginIndexedField().getName()),
                table.getField(index.getEndIndexedField().getName()),
                toSortedFieldIds(index.getHashedFields().stream().map(Field::getName).toArray(String[]::new), table)
        );
    }

    static DBRangeIndex buildIndex(TRangeIndex index, DBTable table) throws SchemaException {
        return new DBRangeIndex(
                table.getField(index.getBeginField()),
                table.getField(index.getEndField()),
                toSortedFieldIds(index.getHashedFields(), table)
        );
    }

    private static DBField[] toSortedFieldIds(String[] fieldNames, DBTable table) throws SchemaException {
        DBField[] result = new DBField[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            result[i] = table.getField(fieldNames[i]);
        }
        Arrays.sort(result, Comparator.comparing(DBObject::getId));
        return result;
    }

    private static String[] toFieldNames(int[] fieldIds, DBTable table) throws SchemaException {
        return Arrays.stream(fieldIds).mapToObj(value -> getFieldName(value, table)).toArray(String[]::new);
    }
}
