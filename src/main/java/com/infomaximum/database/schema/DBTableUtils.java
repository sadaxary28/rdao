package com.infomaximum.database.schema;

import com.infomaximum.database.exception.FieldNotFoundException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.schema.table.THashIndex;
import com.infomaximum.database.schema.table.TIntervalIndex;
import com.infomaximum.database.schema.table.TPrefixIndex;
import com.infomaximum.database.schema.table.TRangeIndex;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
        return table.getFields().stream()
                .filter(field -> field.getId() == fieldId)
                .findFirst()
                .orElseThrow(() -> new FieldNotFoundException(fieldId, table.getName()))
                .getName();
    }

    static DBHashIndex buildIndex(THashIndex index, DBTable table) throws SchemaException {
        return new DBHashIndex(toSortedFields(index.getFields(), table));
    }

    static DBPrefixIndex buildIndex(TPrefixIndex index, DBTable table) throws SchemaException {
        return new DBPrefixIndex(toSortedFields(index.getFields(), table));
    }

    static DBIntervalIndex buildIndex(TIntervalIndex index, DBTable table) throws SchemaException {
        return new DBIntervalIndex(
                table.getField(index.getIndexedField()),
                toSortedFields(index.getHashedFields(), table)
        );
    }

    static DBRangeIndex buildIndex(TRangeIndex index, DBTable table) throws SchemaException {
        return new DBRangeIndex(
                table.getField(index.getBeginField()),
                table.getField(index.getEndField()),
                toSortedFields(index.getHashedFields(), table)
        );
    }

    private static List<DBField> toSortedFields(String[] fieldNames, DBTable table) throws SchemaException {
        return Arrays.stream(fieldNames)
                .map(table::getField)
                .sorted(Comparator.comparing(DBObject::getId))
                .collect(Collectors.toList());
    }

    private static String[] toFieldNames(int[] fieldIds, DBTable table) throws SchemaException {
        return Arrays.stream(fieldIds).mapToObj(value -> getFieldName(value, table)).toArray(String[]::new);
    }
}
