package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.exception.TableNotFoundException;
import com.infomaximum.database.utils.SchemaTableCache;
import net.minidev.json.JSONArray;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBSchema {

    private final String version;
    private final List<DBTable> tables;

    private final SchemaTableCache schemaTableCache;

    private DBSchema(String version, List<DBTable> tables) {
        this.version = version;
        this.tables = tables;
        this.schemaTableCache = new SchemaTableCache(tables, this);
    }

    public String getVersion() {
        return version;
    }

    public List<DBTable> getTables() {
        return tables;
    }

    public DBTable newTable(String name, String namespace, List<DBField> columns) {
        DBTable dbTable = new DBTable(nextId(tables), name, namespace, columns);
        tables.add(dbTable);
        schemaTableCache.newTable(dbTable);
        return dbTable;
    }

    public int findTableIndex(String tableName, String tableNamespace) throws SchemaException {
        DBTable dbTable;
        for (int i = 0; i < tables.size(); ++i) {
            dbTable = tables.get(i);
            if (dbTable.getName().equals(tableName) && dbTable.getNamespace().equals(tableNamespace)) {
                return i;
            }
        }
        return -1;
    }

    public DBTable getTableById(int id) throws SchemaException {
        if (tables.size() <= id) {
            throw new TableNotFoundException("Table with id: " + id + " doesn't found");
        }
        return tables.get(id);
    }

    public List<DBTable> getTablesByNamespace(String namespace) throws SchemaException {
        return tables.stream().filter(table ->  table.getNamespace().equals(namespace)).collect(Collectors.toList());
    }

    public DBTable getTable(String name, String namespace) throws SchemaException {
        DBTable table = schemaTableCache.getTable(name, namespace);
        if (table == null) {
            throw new TableNotFoundException(namespace + "." + name);
        }
        return table;
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
                .orElse(-1) + 1;
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
