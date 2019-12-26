package com.infomaximum.database.schema.newschema;

import com.infomaximum.database.exception.FieldNotFoundException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.schema.newschema.dbstruct.*;

import java.util.Arrays;

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

    static DBHashIndex buildIndex(HashIndex index, DBTable table) throws SchemaException {
        return new DBHashIndex(toSortedFieldIds(index.getFieldNames(), table));
    }

    static DBPrefixIndex buildIndex(PrefixIndex index, DBTable table) throws SchemaException {
        return new DBPrefixIndex(toSortedFieldIds(index.getFieldNames(), table));
    }

    static DBIntervalIndex buildIndex(IntervalIndex index, DBTable table) throws SchemaException {
        return new DBIntervalIndex(
                table.getField(index.getIndexedField().getName()).getId(),
                toSortedFieldIds(index.getHashedFields().stream().map(Field::getName).toArray(String[]::new), table)
        );
    }

    static DBRangeIndex buildIndex(RangeIndex index, DBTable table) throws SchemaException {
        return new DBRangeIndex(
                table.getField(index.getBeginIndexedField().getName()).getId(),
                table.getField(index.getEndIndexedField().getName()).getId(),
                toSortedFieldIds(index.getHashedFields().stream().map(Field::getName).toArray(String[]::new), table)
        );
    }

    private static int[] toSortedFieldIds(String[] fieldNames, DBTable table) throws SchemaException {
        int[] ids = new int[fieldNames.length];
        for (int i = 0; i < fieldNames.length; ++i) {
            ids[i] = table.getField(fieldNames[i]).getId();
        }
        Arrays.sort(ids);
        return ids;
    }

    private static String[] toFieldNames(int[] fieldIds, DBTable table) throws SchemaException {
        return Arrays.stream(fieldIds).mapToObj(value -> getFieldName(value, table)).toArray(String[]::new);
    }
}
