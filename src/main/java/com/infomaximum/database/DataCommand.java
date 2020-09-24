package com.infomaximum.database;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.provider.DBDataCommand;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBSchema;
import com.infomaximum.database.schema.dbstruct.DBTable;

import java.util.Set;

public class DataCommand extends DataReadCommand{

    private final DBDataCommand dataCommand;

    DataCommand(DBDataCommand dataCommand, DBSchema schema) {
        super(dataCommand, schema);
        this.dataCommand = dataCommand;
    }

    public DBDataCommand getDBCommand() {
        return dataCommand;
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
