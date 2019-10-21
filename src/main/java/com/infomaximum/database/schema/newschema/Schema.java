package com.infomaximum.database.schema.newschema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.*;
import com.infomaximum.database.exception.runtime.IllegalTypeException;
import com.infomaximum.database.provider.*;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.schema.newschema.dbstruct.*;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.rocksdb.RocksDBProvider;
import com.infomaximum.rocksdb.SequenceManager;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Все методы по изменению схемы не транзакционны.
 * Если возникнет ошибка при изменении схемы, то объектное описание схемы в памяти,
 * ее описание на диске и структура данных будут несогласованны
 */
public class Schema {

    public static Set<Class<? extends Serializable>> SUPPORTED_FIELD_TYPES = new HashSet<>(Arrays.asList(
            String.class, byte[].class,
            Long.class, Integer.class, Boolean.class, Double.class,
            Instant.class, LocalDateTime.class
    ));

    static final String CURRENT_VERSION = "1.0.0";

    public static final String SERVICE_COLUMN_FAMILY = "service";
    static final byte[] VERSION_KEY = TypeConvert.pack("version");
    static final byte[] SCHEMA_KEY = TypeConvert.pack("schema");

    private final DBProvider dbProvider;
    private final DBSchema dbSchema;
    private final SequenceManager sequenceManager;
    private final ConcurrentMap<Class<? extends DomainObject>, StructEntity> objTables = new ConcurrentHashMap<>();

    private Schema(RocksDBProvider dbProvider, DBSchema schema) throws DatabaseException {
        this.dbProvider = dbProvider;
        this.dbSchema = schema;
        this.sequenceManager = new SequenceManager(dbProvider);
    }

    public static Schema create(RocksDBProvider dbProvider) throws DatabaseException {
        return new Schema(dbProvider, createSchema(dbProvider));
    }

    public static Schema read(RocksDBProvider dbProvider) throws DatabaseException {
        return new Schema(dbProvider, readSchema(dbProvider));
    }

    private static DBSchema createSchema(DBProvider dbProvider) throws DatabaseException {
        dbProvider.createColumnFamily(SERVICE_COLUMN_FAMILY);

        String version = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, VERSION_KEY));
        String schemaJson = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, SCHEMA_KEY));
        if (version != null || schemaJson != null) {
            throw new SchemaException("Schema already exists");
        }

        DBSchema newSchema = DBSchema.fromStrings(CURRENT_VERSION, "[]");
        saveSchema(newSchema, dbProvider);
        return newSchema;
    }

    private static DBSchema readSchema(DBProvider dbProvider) throws DatabaseException {
        String version = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, VERSION_KEY));
        String schemaJson = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, SCHEMA_KEY));
        if (version == null) {
            if (schemaJson == null) {
                throw new SchemaException("Schema not found");
            }
            throw new CorruptedException("Key 'version' not found");
        } else if (schemaJson == null) {
            throw new CorruptedException("Key 'schema' not found");
        }

        if (!CURRENT_VERSION.equals(version)) {
            throw new SchemaException("Incorrect version of the database (" + version + "). Current version is " + CURRENT_VERSION + ".");
        }

        return DBSchema.fromStrings(version, schemaJson);
    }

    private static void saveSchema(DBSchema schema, DBProvider dbProvider) throws DatabaseException {
        try (DBTransaction transaction = dbProvider.beginTransaction()) {
            transaction.put(SERVICE_COLUMN_FAMILY, VERSION_KEY, TypeConvert.pack(schema.getVersion()));
            transaction.put(SERVICE_COLUMN_FAMILY, SCHEMA_KEY, TypeConvert.pack(schema.toTablesJsonString()));
            transaction.commit();
        }
    }

    public DBProvider getDbProvider() {
        return dbProvider;
    }

    public DBSchema getDbSchema() {
        return dbSchema;
    }

    public SequenceManager getSequenceManager() {
        return sequenceManager;
    }
//
//    @SuppressWarnings("unchecked")
//    public <T extends DomainObject> StructEntity resolve(Class<T> objClass) throws SchemaException {
//        return objTables.computeIfAbsent(objClass, this::buildObjTable);
//    }

//    private <T extends DomainObject> StructEntity buildObjTable(Class<T> objClass) throws SchemaException {
//        StructEntity entity = new StructEntity(objClass);
//        Table table = TableUtils.buildTable(entity);
//        DBTable dbTable = getDbSchema().getTable(table.getName());
//        if (!DBTableUtils.buildTable(dbTable, dbSchema).equals(table)) {
//            throw new SchemaException("Class annotation of " + objClass.getSimpleName() + " not match the schema in the database");
//        }
//
//        DBField[] orderedFields = new DBField[entity.fields().length];
//        for (com.infomaximum.database.domainobject.anotation.Field field: entity.fields()) {
//            if (orderedFields[field.number()] != null) {
//                throw new FieldAlreadyExistsException(field.number(), objClass);
//            }
//            orderedFields[field.number()] = dbTable.getField(field.name());
//        }
//
//        return new ObjInfo<>(dbTable, getConstructor(objClass), orderedFields);
//    }

    private static <T extends DomainObject> Constructor<T> getConstructor(Class<T> clazz) {
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(long.class, StructEntity.class);
            constructor.setAccessible(true);
            return constructor;
        } catch (ReflectiveOperationException e) {
            throw new IllegalTypeException(e);
        }
    }

    public void createTable(StructEntity table) throws DatabaseException {
        int tableIndex = dbSchema.findTableIndex(table.getName(), table.getNamespace());
        DBTable dbTable;
        if (tableIndex == -1) {
            dbTable = dbSchema.newTable(table.getName(), table.getNamespace(), new ArrayList<>());

            dbProvider.createColumnFamily(dbTable.getDataColumnFamily());
            dbProvider.createColumnFamily(dbTable.getIndexColumnFamily());
            sequenceManager.createSequence(dbTable.getName());
        } else {
            throw new TableAlreadyExistsException(dbSchema.getTables().get(tableIndex));
        }

        for (Field tableField : table.getFields()) {
            createField(tableField, dbTable, table);
        }

        for (HashIndex index : table.getHashIndexes()) {
            createIndex(index, dbTable);
        }

        for (PrefixIndex index : table.getPrefixIndexes()) {
            createIndex(index, dbTable);
        }

        for (IntervalIndex index : table.getIntervalIndexes()) {
            createIndex(index, dbTable);
        }

        for (RangeIndex index : table.getRangeIndexes()) {
            createIndex(index, dbTable);
        }

        saveSchema();
    }
//
//    public boolean dropTable(String name) throws DatabaseException {
//        int i = dbSchema.findTableIndex(name);
//        if (i == -1) {
//            return false;
//        }
//
//        DBTable table = dbSchema.getTables().remove(i);
//        dbProvider.dropColumnFamily(table.getDataColumnFamily());
//        dbProvider.dropColumnFamily(table.getIndexColumnFamily());
//        sequenceManager.dropSequence(table);
//
//        removeObjTable(name);
//
//        saveSchema();
//        return true;
//    }
//
//    public void checkIntegrity() throws DatabaseException {
//        dbSchema.checkIntegrity();
//
//        for (DBTable table : dbSchema.getTables()) {
//            if (!dbProvider.containsColumnFamily(table.getDataColumnFamily())) {
//                throw new SchemaException("ColumnFamily '" + table.getDataColumnFamily() + "' not found, table='" + table.getName() + "'");
//            }
//
//            if (!dbProvider.containsColumnFamily(table.getIndexColumnFamily())) {
//                throw new SchemaException("ColumnFamily '" + table.getIndexColumnFamily() + "' not found, table='" + table.getName() + "'");
//            }
//
//            if (!sequenceManager.getSequences().containsKey(table.getId())) {
//                throw new SequenceNotFoundException(table);
//            }
//        }
//
//        Set<Integer> existingTableIds = dbSchema.getTables().stream().map(DBObject::getId).collect(Collectors.toSet());
//        sequenceManager.getSequences().keySet().stream()
//                .filter(fieldId -> !existingTableIds.contains(fieldId))
//                .findFirst()
//                .ifPresent(tableId -> {
//                    throw new SchemaException("Table id=" + tableId + " not found");
//                });
//    }
//
//    public List<Table> getTables() {
//        return dbSchema.getTables().stream()
//                .map(t -> DBTableUtils.buildTable(t, dbSchema))
//                .collect(Collectors.toList());
//    }
//
//    public void renameTable(String oldName, String newName) throws DatabaseException {
//        dbSchema.getTable(oldName).setName(newName);
//        saveSchema();
//    }
//
    public void createField(Field tableField, String tableName, String namespace, StructEntity table) throws DatabaseException {
        createField(tableField, dbSchema.getTable(tableName, namespace), table);
        saveSchema();
    }

    private DBField createField(Field tableField, DBTable dbTable, StructEntity table) throws DatabaseException {
        int i = dbTable.findFieldIndex(tableField.getName());
        if (i != -1) {
            throw new FieldAlreadyExistsException(tableField.getName(), dbTable.getName());
        }

        Integer fTableId = tableField.getForeignDependency() != null
                ? dbSchema.getTable(tableField.getForeignDependency().getName(), tableField.getForeignDependency().getNamespace()).getId()
                : null;
        DBField newField = dbTable.newField(tableField.getName(), tableField.getType(), fTableId);
        if (newField.isForeignKey()) {
            createIndex(new HashIndex(tableField, table), dbTable);
        }
        return newField;
    }
//
//    public boolean dropField(String fieldName, String tableName) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName);
//        int i = table.findFieldIndex(fieldName);
//        if (i == -1) {
//            return false;
//        }
//
//        DBField field = table.getFields().get(i);
//        dropIndexesByField(field, table.getHashIndexes(), index -> dropIndexData(index, table));
//        dropIndexesByField(field, table.getPrefixIndexes(), index -> dropIndexData(index, table));
//        dropIndexesByField(field, table.getIntervalIndexes(), index -> dropIndexData(index, table));
//        dropIndexesByField(field, table.getRangeIndexes(), index -> dropIndexData(index, table));
//
//        dropFieldData(field, table);
//
//        table.getFields().remove(i);
//        removeObjTable(tableName);
//
//        saveSchema();
//        return true;
//    }
//
//    private <T extends DBIndex> void dropIndexesByField(DBField field, List<T> indexes, Consumer<T> dropIndex) throws DatabaseException {
//        for (int i = indexes.size() - 1; i > -1; --i) {
//            T index = indexes.get(i);
//            if (index.fieldContains(field.getId())) {
//                dropIndex.accept(index);
//                indexes.remove(i);
//            }
//        }
//    }
//
//    public void renameField(String oldName, String newName, String tableName) throws DatabaseException {
//        dbSchema.getTable(tableName).getField(oldName).setName(newName);
//        saveSchema();
//    }

    public void createIndex(HashIndex index, String tableName, String namespace) throws DatabaseException {
        createIndex(index, dbSchema.getTable(tableName, namespace));
        saveSchema();
    }

    private void createIndex(HashIndex index, DBTable table) {
        DBHashIndex dbIndex = DBTableUtils.buildIndex(index, table);
        if (table.getHashIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            table.attachIndex(dbIndex);

            //TODO Indexing data
        } else if (index.sortedFields.size() != 1 || !table.getField(index.sortedFields.get(0).getName()).isForeignKey()) {
            throw new IndexAlreadyExistsException(index);
        }
    }

    public void createIndex(PrefixIndex index, String tableName, String namespace) throws DatabaseException {
        createIndex(index, dbSchema.getTable(tableName, namespace));
        saveSchema();
    }

    private void createIndex(PrefixIndex index, DBTable table) throws DatabaseException {
        DBPrefixIndex dbIndex = DBTableUtils.buildIndex(index, table);
        if (table.getPrefixIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            table.attachIndex(dbIndex);

            //TODO Indexing data
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

    public void createIndex(IntervalIndex index, String tableName, String namespace) throws DatabaseException {
        createIndex(index, dbSchema.getTable(tableName, namespace));
        saveSchema();
    }

    private void createIndex(IntervalIndex index, DBTable table) throws DatabaseException {
        //todo Indexing data
        DBIntervalIndex dbIndex = DBTableUtils.buildIndex(index, table);
        if (table.getIntervalIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            table.attachIndex(dbIndex);
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

    public void createIndex(RangeIndex index, String tableName, String namespace) throws DatabaseException {
        createIndex(index, dbSchema.getTable(tableName, namespace));
        saveSchema();
    }

    private void createIndex(RangeIndex index, DBTable table) throws DatabaseException {
        DBRangeIndex dbIndex = DBTableUtils.buildIndex(index, table);
        if (table.getRangeIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            table.attachIndex(dbIndex);

            //TODO Indexing data
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

//    public boolean dropIndex(HashIndex index, String tableName) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName);
//        if (index.getFields().length == 1 && table.getField(index.getFields()[0]).isForeignKey()) {
//            return true;
//        }
//
//        DBHashIndex targetIndex = DBTableUtils.buildIndex(index, table);
//        return dropIndex(table.getHashIndexes(), targetIndex::fieldsEquals, dbIndex -> dropIndexData(dbIndex, table), table);
//    }
//
//    private <T extends DBIndex> boolean dropIndex(List<T> indexes, Predicate<T> predicate, Consumer<T> dropIndexData, DBTable table) throws DatabaseException {
//        for (int i = 0; i < indexes.size(); ++i) {
//            T dbIndex = indexes.get(i);
//            if (predicate.test(dbIndex)) {
//                dropIndexData.accept(dbIndex);
//                indexes.remove(i);
//
//                removeObjTable(table.getName());
//
//                saveSchema();
//                return true;
//            }
//        }
//        return false;
//    }
//
//    public boolean dropIndex(PrefixIndex index, String tableName) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName);
//        DBPrefixIndex targetIndex = DBTableUtils.buildIndex(index, table);
//        return dropIndex(table.getPrefixIndexes(), targetIndex::fieldsEquals, dbIndex -> dropIndexData(dbIndex, table), table);
//    }
//
//    public boolean dropIndex(IntervalIndex index, String tableName) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName);
//        DBIntervalIndex targetIndex = DBTableUtils.buildIndex(index, table);
//        return dropIndex(table.getIntervalIndexes(), targetIndex::fieldsEquals, dbIndex -> dropIndexData(dbIndex, table), table);
//    }
//
//    public boolean dropIndex(RangeIndex index, String tableName) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName);
//        DBRangeIndex targetIndex = DBTableUtils.buildIndex(index, table);
//        return dropIndex(table.getRangeIndexes(), targetIndex::fieldsEquals, dbIndex -> dropIndexData(dbIndex, table), table);
//    }
//
    private void saveSchema() throws DatabaseException {
        saveSchema(dbSchema, dbProvider);
    }
//
//    private void dropFieldData(DBField field, DBTable table) throws DatabaseException {
//        try (DBTransaction transaction = dbProvider.beginTransaction()) {
//            try (DBIterator i = transaction.createIterator(table.getDataColumnFamily())) {
//                KeyPattern pattern = new KeyPattern(new KeyPattern.Postfix[] {
//                        new KeyPattern.Postfix(FieldKey.ID_BYTE_SIZE, TypeConvert.pack(field.getId()))
//                });
//
//                for (KeyValue keyValue = i.seek(pattern); keyValue != null; keyValue = i.next()) {
//                    transaction.singleDelete(table.getDataColumnFamily(), keyValue.getKey());
//                }
//            }
//
//            transaction.commit();
//        }
//    }
//
//    private void dropIndexData(DBIndex index, DBTable table) throws DatabaseException {
//        try (DBTransaction transaction = dbProvider.beginTransaction()) {
//            byte[] beginKey = TypeConvert.pack(index.getId());
//            byte[] endKey = TypeConvert.pack(index.getId() + 1);
//
//            transaction.singleDeleteRange(table.getIndexColumnFamily(), beginKey, endKey);
//            transaction.commit();
//        }
//    }
//
//    private void removeObjTable(String tableName) {
//        Iterator<Map.Entry<Class<? extends DomainObject_new>, ObjInfo>> i = objTables.entrySet().iterator();
//        while (i.hasNext()) {
//            ObjInfo table = i.next().getValue();
//            if (table.getTable().getName().equals(tableName)) {
//                i.remove();
//            }
//        }
//    }
}
