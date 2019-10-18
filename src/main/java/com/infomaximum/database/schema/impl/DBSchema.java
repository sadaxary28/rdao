package com.infomaximum.database.schema.impl;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.exception.TableNotFoundException;
import net.minidev.json.JSONArray;

import java.util.List;
import java.util.stream.Stream;

public class DBSchema {

    private final String version;
    private final List<DBTable> tables;

    private DBSchema(String version, List<DBTable> tables) {
        this.version = version;
        this.tables = tables;
    }

    public String getVersion() {
        return version;
    }

    public List<DBTable> getTables() {
        return tables;
    }

    public DBTable newTable(String name, List<DBField> columns) {
        DBTable dbTable = new DBTable(nextId(tables), name, columns);
        tables.add(dbTable);
        return dbTable;
    }

    public int findTableIndex(String tableName) throws SchemaException {
        for (int i = 0; i < tables.size(); ++i) {
            if (tables.get(i).getName().equals(tableName)) {
                return i;
            }
        }
        return -1;
    }

    public DBTable getTable(String name) throws SchemaException {
        int i = findTableIndex(name);
        if (i == -1) {
            throw new TableNotFoundException(name);
        }
        return getTables().get(i);
    }

    public void checkIntegrity() throws SchemaException {
        checkUniqueId(tables);

        for (DBTable table : tables) {
            table.checkIntegrity();
        }
    }

    public static DBSchema fromStrings(String version, String tablesJson) throws SchemaException {
        return new DBSchema(version, JsonUtils.toList(JsonUtils.parse(tablesJson, JSONArray.class), DBTable::fromJson));
    }

    public String toTablesJsonString() {
        return JsonUtils.toJsonArray(tables).toJSONString();
    }

    static int nextId(List<? extends DBObject> items) {
        return nextId(items.stream());
    }

    static int nextId(Stream<? extends DBObject> items) {
        return items
                .map(DBObject::getId)
                .max(Integer::compare)
                .orElse(0) + 1;
    }

    static <T extends DBObject> void checkUniqueId(List<T> objects) throws SchemaException {
        for (int i = 0; i < objects.size(); ++i) {
            T obj = objects.get(i);

            for (int j = i + 1; j < objects.size(); ++j) {
                if (objects.get(j).getId() == obj.getId()) {
                    throw new SchemaException("Not unique id=" + obj.getId() + " for " + obj.getClass().getSimpleName());
                }
            }
        }
    }
}
