package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.*;
import com.infomaximum.database.exception.runtime.*;
import com.infomaximum.database.provider.*;
import com.infomaximum.database.schema.dbstruct.*;
import com.infomaximum.database.schema.table.*;
import com.infomaximum.database.utils.IndexService;
import com.infomaximum.database.utils.TableUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * Все методы по изменению схемы не транзакционны.
 * Если возникнет ошибка при изменении схемы, то объектное описание схемы в памяти,
 * ее описание на диске и структура данных будут несогласованны
 */
public class Schema {

    private final static Logger log = LoggerFactory.getLogger(Schema.class);

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
    private final static ConcurrentMap<Class<? extends DomainObject>, StructEntity> objTables = new ConcurrentHashMap<>();
    private final static ConcurrentMap<TableReference, Class<? extends DomainObject>> tableClasses = new ConcurrentHashMap<>();

    private Schema(DBProvider dbProvider, DBSchema schema) {
        this.dbProvider = dbProvider;
        this.dbSchema = schema;
    }

    public static Schema create(DBProvider dbProvider) throws DatabaseException {
        return new Schema(dbProvider, createSchema(dbProvider));
    }

    public static Schema read(DBProvider dbProvider) throws DatabaseException {
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

    public static boolean exists(DBProvider dbProvider) throws DatabaseException {
        return dbProvider.containsColumnFamily(SERVICE_COLUMN_FAMILY);
    }

    private static DBSchema readSchema(DBProvider dbProvider) throws DatabaseException {
        String version = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, VERSION_KEY));
        String schemaJson = TypeConvert.unpackString(dbProvider.getValue(SERVICE_COLUMN_FAMILY, SCHEMA_KEY));
        validateSchema(version, schemaJson);
        return DBSchema.fromStrings(version, schemaJson);
    }

    private static void validateSchema(String version, String schemaJson) throws DatabaseException {
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

    @Deprecated
    public boolean existTable(StructEntity table) {
        return dbSchema.findTableIndex(table.getName(), table.getNamespace()) != -1;
    }

    public void createTable(Table table) throws DatabaseException {
        int tableIndex = dbSchema.findTableIndex(table.getName(), table.getNamespace());
        DBTable dbTable;
        if (tableIndex == -1) {
            dbTable = dbSchema.newTable(table.getName(), table.getNamespace(), new ArrayList<>());

            dbProvider.createColumnFamily(dbTable.getDataColumnFamily());
            dbProvider.createColumnFamily(dbTable.getIndexColumnFamily());
            dbProvider.createSequence(dbTable.getDataColumnFamily());
        } else {
            throw new TableAlreadyExistsException(dbSchema.getTables().get(tableIndex));
        }
        for (TField tableField : table.getFields()) {
            createField(tableField, dbTable);
        }
        for (THashIndex index : table.getHashIndexes()) {
            createIndex(index, dbTable);
        }
        for (TPrefixIndex index : table.getPrefixIndexes()) {
            createIndex(index, dbTable);
        }
        for (TIntervalIndex index : table.getIntervalIndexes()) {
            createIndex(index, dbTable);
        }
        for (TRangeIndex index : table.getRangeIndexes()) {
            createIndex(index, dbTable);
        }
        saveSchema();
    }

    public Table getTable(String name, String namespace) {
        DBTable dbTable = dbSchema.getTable(name, namespace);
        return TableUtils.buildTable(dbTable, dbSchema);
    }

    @Deprecated
    public void createTable(StructEntity table) throws DatabaseException {
        Schema.resolve(table.getObjectClass());
        int tableIndex = dbSchema.findTableIndex(table.getName(), table.getNamespace());
        DBTable dbTable;
        if (tableIndex == -1) {
            dbTable = dbSchema.newTable(table.getName(), table.getNamespace(), new ArrayList<>());

            dbProvider.createColumnFamily(dbTable.getDataColumnFamily());
            dbProvider.createColumnFamily(dbTable.getIndexColumnFamily());
            dbProvider.createSequence(dbTable.getDataColumnFamily());
        } else {
            throw new TableAlreadyExistsException(dbSchema.getTables().get(tableIndex));
        }
        for (Field tableField : table.getFields()) {
            createField(tableField, dbTable, table);
        }
        for (HashIndex index : table.getHashIndexes()) {
            createIndex(index, dbTable, table);
        }
        for (PrefixIndex index : table.getPrefixIndexes()) {
            createIndex(index, dbTable, table);
        }
        for (IntervalIndex index : table.getIntervalIndexes()) {
            createIndex(index, dbTable, table);
        }
        for (RangeIndex index : table.getRangeIndexes()) {
            createIndex(index, dbTable, table);
        }
        saveSchema();
    }

    public boolean dropTable(String name, String namespace) throws DatabaseException {
        int i = dbSchema.findTableIndex(name, namespace);
        if (i == -1) {
            return false;
        }

        DBTable table = dbSchema.getTables().remove(i);
        if (hasDependenceOfOtherTable(table.getId())) {
            throw new TableRemoveException("Can't remove table: " + namespace + "." + name + ", there are dependencies on the table");
        }
        dbProvider.dropColumnFamily(table.getDataColumnFamily());
        dbProvider.dropColumnFamily(table.getIndexColumnFamily());
        dbProvider.dropSequence(table.getDataColumnFamily());

        removeObjTable(name, namespace);

        saveSchema();
        return true;
    }

    public static StructEntity getEntity(Class<? extends DomainObject> clazz) {
        StructEntity entity = objTables.get(clazz);
        if (entity == null) {
            entity = objTables.get(StructEntity.getAnnotationClass(clazz));
            if (entity == null) {
                throw new SchemaException("StructEntity doesn't initialized: " + clazz);
            }
            objTables.putIfAbsent(clazz, entity);
        }
        return entity;
    }

    public static Class<? extends DomainObject> getTableClass(String name, String namespace) {
        return tableClasses.get(new TableReference(name, namespace));
    }

    public static <T extends DomainObject> StructEntity resolve(Class<T> objClass) throws SchemaException {
        Class<? extends DomainObject> annotationClass = StructEntity.getAnnotationClass(objClass);
        StructEntity entity = objTables.get(annotationClass);
        if (entity == null) {
            entity = new StructEntity(annotationClass);
            objTables.put(annotationClass, entity);
        }
        TableReference tableReference = new TableReference(entity.getName(), entity.getNamespace());
        tableClasses.putIfAbsent(tableReference, annotationClass);
        return entity;
    }

    private static <T extends DomainObject> StructEntity buildObjTable(Class<T> objClass) throws SchemaException {
        return new StructEntity(objClass);
    }

    public Collection<StructEntity> getDomains() {
        return objTables.values();
    }

    public void checkIntegrity() throws DatabaseException {
        dbSchema.checkIntegrity();

        //todo V.Bukharkin add check with cf in db for exclude
        Set<String> processedNames = new HashSet<>();
        for (DBTable table : dbSchema.getTables()) {
            if (processedNames.contains(table.getDataColumnFamily())) {
                throw new InconsistentDatabaseException("Column family " + table.getNamespace() + " into " + table.getName() + " already exists.");
            }
            processedNames.add(table.getNamespace());
            if (!dbProvider.containsColumnFamily(table.getDataColumnFamily())) {
                throw new SchemaException("ColumnFamily '" + table.getDataColumnFamily() + "' not found, table='" + table.getName() + "'");
            }
            if (!dbProvider.containsColumnFamily(table.getIndexColumnFamily())) {
                throw new SchemaException("ColumnFamily '" + table.getIndexColumnFamily() + "' not found, table='" + table.getName() + "'");
            }
            if (!dbProvider.containsSequence(table.getDataColumnFamily())) {
                throw new SequenceNotFoundException(table.getDataColumnFamily());
            }
        }
    }
//
//    public void renameTable(String oldName, String newName, String namespace) throws DatabaseException {
//        dbSchema.getTable(oldName, namespace).setName(newName);
//        saveSchema();
//    }

    @Deprecated
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
            createIndex(new HashIndex(tableField, table), dbTable, table);
        }
        saveSchema();
        return newField;
    }

    private DBField createField(TField tableField, DBTable dbTable) throws DatabaseException {
        int i = dbTable.findFieldIndex(tableField.getName());
        if (i != -1) {
            throw new FieldAlreadyExistsException(tableField.getName(), dbTable.getName());
        }

        Integer fTableId = tableField.getForeignTable() != null
                ? dbSchema.getTable(tableField.getForeignTable().getName(), tableField.getForeignTable().getNamespace()).getId()
                : null;
        DBField newField = dbTable.newField(tableField.getName(), tableField.getType(), fTableId);
        if (newField.isForeignKey()) {
            createIndex(new THashIndex(tableField.getName()), dbTable);
        }
        saveSchema();
        return newField;
    }

    public void createField(TField tableField, Table table) throws DatabaseException {
        DBTable dbTable = dbSchema.getTable(table.getName(), table.getNamespace());
        createField(tableField, dbTable);
    }

    public void createField(TField tableField, String tableName, String tableNamespace) throws DatabaseException {
        DBTable dbTable = dbSchema.getTable(tableName, tableNamespace);
        createField(tableField, dbTable);
    }

//    public boolean dropField(String fieldName, String tableName, String namespace) throws DatabaseException {
//        DBTable table = dbSchema.getTable(tableName, namespace);
//        int i = table.findFieldIndex(fieldName);
//        if (i == -1) {
//            return false;
//        }
//
//        DBField field = table.getSortedFields().get(i);
//        dropIndexesByField(field, table.getHashIndexes(), table);
//        dropIndexesByField(field, table.getPrefixIndexes(), table);
//        dropIndexesByField(field, table.getIntervalIndexes(), table);
//        dropIndexesByField(field, table.getRangeIndexes(), table);
//
//        dropFieldData(field, table);
//
//        table.dropField(i);
//
//        saveSchema();
//        return true;
//    }

//    private <T extends DBIndex> void dropIndexesByField(DBField field, List<T> indexes, DBTable table) throws DatabaseException {
//        for (int i = indexes.size() - 1; i > -1; --i) {
//            T index = indexes.get(i);
//            if (index.fieldContains(field.getId())) {
//                dropIndexData(index, table);
//                indexes.remove(i);
//            }
//        }
//    }

    public void renameField(String oldName, String newName, String tableName, String namespace) throws DatabaseException {
        dbSchema.getTable(tableName, namespace).getField(oldName).setName(newName);
        saveSchema();
    }

    @Deprecated
    private void createIndex(HashIndex index, DBTable dbTable, StructEntity table) throws DatabaseException {
        DBHashIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getHashIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doIndex(index, table, dbProvider);
            saveSchema();
        } else if (index.sortedFields.size() != 1 || !dbTable.getField(index.sortedFields.get(0).getName()).isForeignKey()) {
            throw new IndexAlreadyExistsException(index);
        }
    }

    private void createIndex(THashIndex index, DBTable dbTable) throws DatabaseException {
        DBHashIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getHashIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doIndex(dbIndex, dbTable, dbProvider);
            saveSchema();
        } else if (dbIndex.getFieldIds().length != 1 || !dbTable.getField(index.getFields()[0]).isForeignKey()) {
            throw new IndexAlreadyExistsException(dbIndex);
        }
    }

    @Deprecated
    private void createIndex(PrefixIndex index, DBTable dbTable, StructEntity table) throws DatabaseException {
        DBPrefixIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getPrefixIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doPrefixIndex(index, table, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

    private void createIndex(TPrefixIndex index, DBTable dbTable) throws DatabaseException {
        DBPrefixIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getPrefixIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doPrefixIndex(dbIndex, dbTable, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(dbIndex);
        }
    }

    @Deprecated
    private void createIndex(IntervalIndex index, DBTable dbTable, StructEntity table) throws DatabaseException {
        DBIntervalIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getIntervalIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doIntervalIndex(index, table, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

    private void createIndex(TIntervalIndex index, DBTable dbTable) throws DatabaseException {
        DBIntervalIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getIntervalIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doIntervalIndex(dbIndex, dbTable, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(dbIndex);
        }
    }

    @Deprecated
    private void createIndex(RangeIndex index, DBTable dbTable, StructEntity table) throws DatabaseException {
        DBRangeIndex dbIndex = DBTableUtils.buildIndex(index, dbTable);
        if (dbTable.getIntervalIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            dbTable.attachIndex(dbIndex);
            IndexService.doIntervalIndex(index, table, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(index);
        }
    }

    private void createIndex(TRangeIndex index, DBTable table) throws DatabaseException {
        DBRangeIndex dbIndex = DBTableUtils.buildIndex(index, table);
        if (table.getRangeIndexes().stream().noneMatch(dbIndex::fieldsEquals)) {
            table.attachIndex(dbIndex);
            IndexService.doIntervalIndex(dbIndex, table, dbProvider);
            saveSchema();
        } else {
            throw new IndexAlreadyExistsException(dbIndex);
        }
    }

    @Deprecated
    public boolean dropIndex(HashIndex index, String tableName, String namespace) throws DatabaseException {
        DBTable table = dbSchema.getTable(tableName, namespace);
        if (index.sortedFields.size() == 1 && table.getField(index.sortedFields.get(0).getName()).isForeignKey()) {
            return true;
        }
        DBHashIndex targetIndex = DBTableUtils.buildIndex(index, table);
        table.dropIndex(targetIndex);
        return dropIndex(table.getHashIndexes(), targetIndex::fieldsEquals, table);
    }

    private <T extends DBIndex> boolean dropIndex(List<T> indexes, Predicate<T> predicate, DBTable table) throws DatabaseException {
        for (int i = 0; i < indexes.size(); ++i) {
            T dbIndex = indexes.get(i);
            if (predicate.test(dbIndex)) {
                dropIndexData(dbIndex, table);
                saveSchema();
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public boolean dropIndex(PrefixIndex index, String tableName, String namespace) throws DatabaseException {
        DBTable table = dbSchema.getTable(tableName, namespace);
        DBPrefixIndex targetIndex = DBTableUtils.buildIndex(index, table);
        table.dropIndex(targetIndex);
        return dropIndex(table.getPrefixIndexes(), targetIndex::fieldsEquals, table);
    }

    @Deprecated
    public boolean dropIndex(IntervalIndex index, String tableName, String namespace) throws DatabaseException {
        DBTable table = dbSchema.getTable(tableName, namespace);
        DBIntervalIndex targetIndex = DBTableUtils.buildIndex(index, table);
        table.dropIndex(targetIndex);
        return dropIndex(table.getIntervalIndexes(), targetIndex::fieldsEquals, table);
    }

    @Deprecated
    public boolean dropIndex(RangeIndex index, String tableName, String namespace) throws DatabaseException {
        DBTable table = dbSchema.getTable(tableName, namespace);
        DBRangeIndex targetIndex = DBTableUtils.buildIndex(index, table);
        table.dropIndex(targetIndex);
        return dropIndex(table.getRangeIndexes(), targetIndex::fieldsEquals, table);
    }

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

    private void dropIndexData(DBIndex index, DBTable table) throws DatabaseException {
        try (DBTransaction transaction = dbProvider.beginTransaction()) {
            byte[] beginKey = TypeConvert.pack(index.getId());
            byte[] endKey = TypeConvert.pack(index.getId() + 1);

            transaction.singleDeleteRange(table.getIndexColumnFamily(), beginKey, endKey);
            transaction.commit();
        }
    }

    private void removeObjTable(String name, String namespace) {
        Iterator<Map.Entry<Class<? extends DomainObject>, StructEntity>> i = objTables.entrySet().iterator();
        while (i.hasNext()) {
            StructEntity table = i.next().getValue();
            if (table.getName().equals(name) && table.getNamespace().equalsIgnoreCase(namespace)) {
                i.remove();
            }
            tableClasses.remove(new TableReference(name, namespace));
        }
    }

    //todo V.Bukharkin медленный способ, нужно придумать быстрее
    private boolean hasDependenceOfOtherTable(int tableId) {
        for (DBTable table : getDbSchema().getTables()) {
            if(table.getId() == tableId) {
                continue;
            }
            for (DBField field : table.getSortedFields()) {
                if (field.isForeignKey() && field.getForeignTableId() == tableId) {
                    return true;
                }
            }
        }
        return false;
    }
}
