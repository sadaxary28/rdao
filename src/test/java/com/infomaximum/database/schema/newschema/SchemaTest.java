package com.infomaximum.database.schema.newschema;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.schema.DomainDataJ5Test;
import com.infomaximum.database.schema.newschema.dbstruct.DBField;
import com.infomaximum.database.schema.newschema.dbstruct.DBSchema;
import com.infomaximum.database.schema.newschema.dbstruct.DBTable;
import com.infomaximum.database.schema.newschema.dbstruct.DBTableTestUtil;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.ExchangeFolderReadable;
import com.infomaximum.domain.GeneralReadable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;

class SchemaTest extends DomainDataJ5Test {

    @Test
    @DisplayName("Создание пустой схемы")
    void justCreateTest() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        DBSchema schemaDB = schema.getDbSchema();
        Assertions.assertThat(schemaDB.getTables()).isEmpty();
        Assertions.assertThat(schemaDB.getVersion()).isEqualTo(Schema.CURRENT_VERSION);
        rocksDBProvider.containsColumnFamily(Schema.SERVICE_COLUMN_FAMILY);
        String schemaJson = TypeConvert.unpackString(rocksDBProvider.getValue(Schema.SERVICE_COLUMN_FAMILY, Schema.SCHEMA_KEY));
        Assertions.assertThat(schemaJson).isEqualTo("[]");
        String versionJson = TypeConvert.unpackString(rocksDBProvider.getValue(Schema.SERVICE_COLUMN_FAMILY, Schema.VERSION_KEY));
        Assertions.assertThat(versionJson).isEqualTo(Schema.CURRENT_VERSION);
    }

    @Test
    @DisplayName("Ошибка создания уже существующей схемы")
    void schemaAlreadyExistsTest() throws DatabaseException {
        Schema.create(rocksDBProvider);
        Assertions.assertThatExceptionOfType(DatabaseException.class).isThrownBy(() -> Schema.create(rocksDBProvider));
    }

    @Test
    @DisplayName("Чтение пустой схемы")
    void readSchemaTest() throws DatabaseException {
        Schema.create(rocksDBProvider);

        Schema readSchema = Schema.read(rocksDBProvider);
        DBSchema schemaDB = readSchema.getDbSchema();
        Assertions.assertThat(schemaDB.getTables()).isEmpty();
        Assertions.assertThat(schemaDB.getVersion()).isEqualTo(Schema.CURRENT_VERSION);
    }

    @Test
    @DisplayName("Создание простой таблицы с одним полем и одним hashIndex")
    void createSimpleTable() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity generalSE = new StructEntity(GeneralReadable.class);
        schema.createTable(generalSE);

        assertColumnFamilies("com.infomaximum.rocksdb.general", "com.infomaximum.rocksdb.general.index");
        DBTable expected = DBTableTestUtil.buildDBTable(1, "general", "com.infomaximum.rocksdb", new ArrayList<DBField>() {{
                    add(DBTableTestUtil.buildDBField(1, "value", Long.class, null));
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(1));
        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    @Test
    @DisplayName("Создание таблицы с несколькими полями, одним hashIndex и зависимостью на саму себя")
    void createMultiplyFieldTable() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity exchangeFolder = new StructEntity(ExchangeFolderReadable.class);
        schema.createTable(exchangeFolder);

        assertColumnFamilies("com.infomaximum.exchange.ExchangeFolder", "com.infomaximum.exchange.ExchangeFolder.index");
        DBTable expected = DBTableTestUtil.buildDBTable(1, "ExchangeFolder", "com.infomaximum.exchange", new ArrayList<DBField>() {{
                    add(DBTableTestUtil.buildDBField(1, "uuid", String.class, null));
                    add(DBTableTestUtil.buildDBField(2, "email", String.class, null));
                    add(DBTableTestUtil.buildDBField(3, "date", Instant.class, null));
                    add(DBTableTestUtil.buildDBField(4, "state", String.class, null));
                    add(DBTableTestUtil.buildDBField(5, "parent_id", Long.class, 1));
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(5));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(1, 2));
        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    @Test
    @DisplayName("Создание таблицы с несколькими полями, всеми индексами и на другую сущность")
    void createTableWithAllIndexes() throws DatabaseException {
        Schema schema = Schema.create(rocksDBProvider);
        StructEntity general = new StructEntity(GeneralReadable.class);
        schema.createTable(general);
        StructEntity exchangeFolder = new StructEntity(ExchangeFolderReadable.class);
        schema.createTable(exchangeFolder);
        StructEntity storeFile = new StructEntity(StoreFileReadable.class);
        schema.createTable(storeFile);

        assertColumnFamilies("com.infomaximum.store.StoreFile", "com.infomaximum.store.StoreFile.index");
        DBTable expected = DBTableTestUtil.buildDBTable(1, "StoreFile", "com.infomaximum.store", new ArrayList<DBField>() {{
                    add(DBTableTestUtil.buildDBField(1, "name", String.class, null));
                    add(DBTableTestUtil.buildDBField(2, "type", String.class, null));
                    add(DBTableTestUtil.buildDBField(3, "size", Long.class, null));
                    add(DBTableTestUtil.buildDBField(4, "single", Boolean.class, null));
                    add(DBTableTestUtil.buildDBField(5, "format", FormatType.class, null));
                    add(DBTableTestUtil.buildDBField(6, "folder_id", Long.class, 2));
                    add(DBTableTestUtil.buildDBField(7, "double", Double.class, null));
                    add(DBTableTestUtil.buildDBField(8, "begin_time", Instant.class, null));
                    add(DBTableTestUtil.buildDBField(9, "end_time", Instant.class, null));
                    add(DBTableTestUtil.buildDBField(10, "begin", Long.class, null));
                    add(DBTableTestUtil.buildDBField(11, "end", Long.class, null));
                    add(DBTableTestUtil.buildDBField(12, "local_begin", LocalDateTime.class, null));
                    add(DBTableTestUtil.buildDBField(13, "local_end", LocalDateTime.class, null));
                    add(DBTableTestUtil.buildDBField(14, "data", byte[].class, null));
                }});
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(6));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(3));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(1));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(1, 3));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(5));
        expected.attachIndex(DBTableTestUtil.buildDBHashIndex(12));

        expected.attachIndex(DBTableTestUtil.buildDBPrefixIndex(1));
        expected.attachIndex(DBTableTestUtil.buildDBPrefixIndex(1, 2));

        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(3));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(7));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(8));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(12));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(3, 1));
        expected.attachIndex(DBTableTestUtil.buildDBIntervalIndex(3, 6));

        DBTableTestUtil.assertThatContains(rocksDBProvider, expected);
    }

    private void assertColumnFamilies(String... cfs) {
        Assertions.assertThat(rocksDBProvider.getColumnFamilies()).contains(cfs);
    }

    // Добавить еще на чтение


    // Добавить save schema
}
