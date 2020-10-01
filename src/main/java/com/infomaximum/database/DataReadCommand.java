package com.infomaximum.database;

import com.infomaximum.database.domainobject.filter.*;
import com.infomaximum.database.engine.*;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBSchema;
import com.infomaximum.database.schema.dbstruct.DBTable;

import java.util.Set;

public class DataReadCommand {

    private final DBDataReader dataReader;
    protected final DBSchema schema;

    DataReadCommand(DBDataReader dataReader, DBSchema schema) {
        this.dataReader = dataReader;
        this.schema = schema;
    }

    public DBDataReader getDBCommand() {
        return dataReader;
    }

    public RecordIterator select(String table, String namespace) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new AllIterator(dbTable, dataReader);
    }

    public RecordIterator select(String table, String namespace, HashFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new HashIterator(dbTable, filter, dataReader);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, PrefixFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new PrefixIterator(dbTable, toFieldArray(fields, dbTable), filter, dataReader);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, IntervalFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new IntervalIterator(dbTable, toFieldArray(fields, dbTable), filter, dataReader);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, RangeFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new RangeIterator(dbTable, toFieldArray(fields, dbTable), filter, dataReader);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, IdFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new IdIterator(dbTable, toFieldArray(fields, dbTable), filter, dataReader);
    }

    private static DBField[] toFieldArray(Set<String> fieldNames, DBTable table) throws SchemaException {
        DBField[] fields = new DBField[fieldNames.size()];
        int i = 0;
        for (String name : fieldNames) {
            fields[i++] = table.getField(name);
        }
        return fields;
    }
}
