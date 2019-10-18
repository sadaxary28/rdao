package com.infomaximum.database.schema.newschema;

import com.infomaximum.database.exception.ColumnFamilyNotFoundException;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.schema.DomainDataJ5Test;
import com.infomaximum.database.schema.impl.DBSchema;
import com.infomaximum.database.utils.TypeConvert;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("Ошибка чтения несуществующей схемы")
    void readSchemaFailTest() {
        Assertions.assertThatExceptionOfType(ColumnFamilyNotFoundException.class).isThrownBy(() -> Schema.read(rocksDBProvider));
    }

    // Добавить еще на чтение


    // Добавить save schema






}
