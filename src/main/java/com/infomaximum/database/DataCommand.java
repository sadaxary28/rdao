package com.infomaximum.database;

import com.infomaximum.database.domainobject.filter.*;
import com.infomaximum.database.engine.*;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.SchemaException;
import com.infomaximum.database.provider.DBDataCommand;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBSchema;
import com.infomaximum.database.schema.dbstruct.DBTable;

import java.util.Set;

public class DataCommand {

    private final DBDataCommand dataCommand;
    private final DBSchema schema;

    DataCommand(DBDataCommand dataCommand, DBSchema schema) {
        this.dataCommand = dataCommand;
        this.schema = schema;
    }

    public DBDataCommand getDBCommand() {
        return dataCommand;
    }

    public RecordIterator select(String table, String namespace, Set<String> fields) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new AllIterator(dbTable, dataCommand);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, HashFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new HashIterator(dbTable, toFieldArray(fields, dbTable), filter, dataCommand);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, PrefixFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new PrefixIterator(dbTable, toFieldArray(fields, dbTable), filter, dataCommand);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, IntervalFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new IntervalIterator(dbTable, toFieldArray(fields, dbTable), filter, dataCommand);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, RangeFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new RangeIterator(dbTable, toFieldArray(fields, dbTable), filter, dataCommand);
    }

    public RecordIterator select(String table, String namespace, Set<String> fields, IdFilter filter) throws DatabaseException {
        DBTable dbTable = schema.getTable(table, namespace);
        return new IdIterator(dbTable, toFieldArray(fields, dbTable), filter, dataCommand);
    }

    public long insertRecord(String table, String namespace, String[] fields, Object[] values) throws DatabaseException {
        // TODO realize
        return 0;
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

    private static DBField[] toFieldArray(Set<String> fieldNames, DBTable table) throws SchemaException {
        DBField[] fields = new DBField[fieldNames.size()];
        int i = 0;
        for (String name : fieldNames) {
            fields[i++] = table.getField(name);
        }
        return fields;
    }
}
