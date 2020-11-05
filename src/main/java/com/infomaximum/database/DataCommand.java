package com.infomaximum.database;

import com.infomaximum.database.domainobject.Value;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.exception.UnexpectedFieldValueException;
import com.infomaximum.database.provider.DBDataCommand;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.utils.*;
import com.infomaximum.database.utils.key.FieldKey;
import com.infomaximum.database.utils.key.HashIndexKey;
import com.infomaximum.database.utils.key.IntervalIndexKey;
import com.infomaximum.database.utils.key.RangeIndexKey;

import java.io.Serializable;
import java.util.*;

public class DataCommand extends DataReadCommand {

    private final DBDataCommand dataCommand;

    DataCommand(DBDataCommand dataCommand, DBSchema schema) {
        super(dataCommand, schema);
        this.dataCommand = dataCommand;
    }

    public DBDataCommand getDBCommand() {
        return dataCommand;
    }

    public long insertRecord(String tableName, String namespace, String[] fields, Object[] values) throws DatabaseException {
        return insertRecord(tableName,
                namespace,
                TableUtils.sortValuesByFieldOrder(tableName, namespace, fields, values, schema));
    }

    /**
     *
     * @param values должен совпадать с количеством столбцов таблицы RocksDB.
     *               Значения записываются в поля с идентифкатором соответствующего элемента массива values[]
     * @return id созданного объекта или -1, если объект не был создан
     */
    public long insertRecord(String tableName, String namespace, Object[] values) throws DatabaseException {

        if (values == null) {
            return -1;
        }
        DBTable table = schema.getTable(tableName, namespace);
        if (values.length != table.getSortedFields().size()) {
            throw new UnexpectedFieldValueException("Size of inserting values " + values.length + " doesn't equal table field size " + table.getSortedFields().size());
        }
        long id = dataCommand.nextId(table.getDataColumnFamily());

//        final Value<Serializable>[] loadedValues = object.getLoadedValues();
        Record record = new Record(id, values);
        // update hash-indexed values
        for (DBHashIndex index : table.getHashIndexes()) {
            createIndexedValue(index, record, table);
        }

        // update prefix-indexed values
        for (DBPrefixIndex index : table.getPrefixIndexes()) {
            createIndexedValue(index, record, table);
        }

        // update interval-indexed values
        for (DBIntervalIndex index : table.getIntervalIndexes()) {
            createIndexedValue(index, record, table);
        }

        // update range-indexed values
        for (DBRangeIndex index: table.getRangeIndexes()) {
            createIndexedValue(index, record, table);
        }

        // update self-object
        dataCommand.put(table.getDataColumnFamily(), new FieldKey(record.getId()).pack(), TypeConvert.EMPTY_BYTE_ARRAY);
        for (int i = 0; i < values.length; ++i) {
            Object newValue = values[i];
            if (newValue == null) {
                continue;
            }

            DBField field = table.getField(i);

            validateUpdatingValue(record, field, newValue, table);

            byte[] key = new FieldKey(record.getId(), TypeConvert.pack(field.getName())).pack();
            byte[] bValue = TypeConvert.pack(field.getType(), newValue, null);
            dataCommand.put(table.getDataColumnFamily(), key, bValue);
        }

        return id;
    }

    public void updateRecord(String table, String namespace, long id, String[] fields, Object[] values) throws DatabaseException {
        // TODO realize
    }

    public void deleteRecord(String table, String namespace, long id) throws DatabaseException {
        // TODO realize
    }

    public void clearTable(String table, String namespace) throws DatabaseException {
        // TODO realize
    }

    private static boolean anyChanged(List<Field> fields, Value<Serializable>[] newValues) {
        for (Field field: fields) {
            if (newValues[field.getNumber()] != null) {
                return true;
            }
        }
        return false;
    }

//    private static void updateIndexedValue(RangeIndex index, DomainObject obj, Value<Serializable>[] prevValues, Value<Serializable>[] newValues, DBTransaction transaction) throws DatabaseException {
//        final List<Field> hashedFields = index.getHashedFields();
//        final RangeIndexKey indexKey = new RangeIndexKey(obj.getId(), new long[hashedFields.size()], index);
//
//        if (!obj._isJustCreated()) {
//            // Remove old value-index
//            HashIndexUtils.setHashValues(hashedFields, prevValues, indexKey.getHashedValues());
//            RangeIndexUtils.removeIndexedRange(index, indexKey,
//                    prevValues[index.getBeginIndexedField().getNumber()].getValue(),
//                    prevValues[index.getEndIndexedField().getNumber()].getValue(),
//                    transaction, transaction::delete);
//        }
//
//        // Add new value-index
//        setHashValues(hashedFields, prevValues, newValues, indexKey.getHashedValues());
//        RangeIndexUtils.insertIndexedRange(index, indexKey,
//                getValue(index.getBeginIndexedField(), prevValues, newValues),
//                getValue(index.getEndIndexedField(), prevValues, newValues),
//                transaction);
//    }

    private void createIndexedValue(DBHashIndex index, Record record, DBTable table) throws DatabaseException {
        final HashIndexKey indexKey = new HashIndexKey(record.getId(), index);

        // Add new value-index
        setHashValues(table.getFields(index.getFieldIds()), record, indexKey.getFieldValues());
        dataCommand.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    private void createIndexedValue(DBPrefixIndex index, Record record, DBTable table) throws DatabaseException {
        List<String> insertingLexemes = new ArrayList<>();
        PrefixIndexUtils.getIndexedLexemes(table.getFields(index.getFieldIds()), record.getValues(), insertingLexemes);
        PrefixIndexUtils.insertIndexedLexemes(index, record.getId(), insertingLexemes, table, dataCommand);
    }

    private void createIndexedValue(DBIntervalIndex index, Record record, DBTable table) throws DatabaseException {
        final DBField[] hashedFields = table.getFields(index.getHashFieldIds());
        final DBField indexedField = table.getField(index.getIndexedFieldId());
        final IntervalIndexKey indexKey = new IntervalIndexKey(record.getId(), new long[hashedFields.length], index);

        // Add new value-index
        setHashValues(hashedFields, record, indexKey.getHashedValues());
        indexKey.setIndexedValue(record.getValues()[indexedField.getId()]);
        dataCommand.put(table.getIndexColumnFamily(), indexKey.pack(), TypeConvert.EMPTY_BYTE_ARRAY);
    }

    private void createIndexedValue(DBRangeIndex index, Record record, DBTable table) throws DatabaseException {
        final DBField[] hashedFields = table.getFields(index.getHashFieldIds());
        final RangeIndexKey indexKey = new RangeIndexKey(record.getId(), new long[hashedFields.length], index);

        // Add new value-index
        setHashValues(hashedFields, record, indexKey.getHashedValues());
        RangeIndexUtils.insertIndexedRange(index,
                indexKey,
                record.getValues()[index.getBeginFieldId()],
                record.getValues()[index.getEndFieldId()],
                table,
                dataCommand);
    }

    private static void setHashValues(DBField[] fields, Record record, long[] destination) {
        for (int i = 0; i < fields.length; ++i) {
            DBField field = fields[i];
//            Object value = getValue(field, prevValues, newValues);
            destination[i] = HashIndexUtils.buildHash(field.getType(), record.getValues()[field.getId()], null);
        }
    }

    private static Object getValue(Field field, Value<Serializable>[] prevValues, Value<Serializable>[] newValues) {
        Value<Serializable> value = newValues[field.getNumber()];
        if (value == null) {
            value = prevValues[field.getNumber()];
        }
        return value.getValue();
    }

    private void validateUpdatingValue(Record record, DBField field, Object value, DBTable table) throws DatabaseException {
        if (value == null) {
            return;
        }

        if (!field.isForeignKey()) {
            return;
        }

        long fkeyIdValue = (Long) value;
        DBTable foreignTable = schema.getTableById(field.getForeignTableId());
        if (dataCommand.getValue(foreignTable.getDataColumnFamily(), new FieldKey(fkeyIdValue).pack()) == null) {
            throw new ForeignDependencyException(record.getId(),
                    table,
                    foreignTable,
                    field,
                    fkeyIdValue);
        }
    }
}
