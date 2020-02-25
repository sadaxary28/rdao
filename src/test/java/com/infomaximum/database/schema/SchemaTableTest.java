package com.infomaximum.database.schema;

import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.Filter;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.FieldAlreadyExistsException;
import com.infomaximum.database.exception.runtime.TableNotFoundException;
import com.infomaximum.database.exception.runtime.TableRemoveException;
import com.infomaximum.database.schema.table.*;
import com.infomaximum.domain.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class SchemaTableTest extends DomainDataJ5Test {

    @BeforeEach
    void setUp() throws DatabaseException {
        schema = Schema.create(rocksDBProvider);
    }

    private Schema schema;

    //Создание таблицы_____________________________________________
    @Test
    @DisplayName("Создает простую таблицу с полями без зависимостей")
    void createTableOnlyFieldsTest() throws DatabaseException {
        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("field1", String.class));
            add(new TField("field2", Integer.class));
            add(new TField("field3", Long.class));
        }};
        Table table = new Table("dataTest", "com.infomaximum.exchange", fields);
        schema.createTable(table);

        assertThatSchemaContainsTable(table);
    }

    @Test
    @DisplayName("Создает простую таблицу с полями без зависимостей и хеш индексом")
    void createTableFieldsAndIndexesTest() throws DatabaseException {
        Schema.resolve(GeneralReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("value", String.class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("value"));
        }};
        Table table = new Table("general", "com.infomaximum.rocksdb", fields, hashIndexes);
        schema.createTable(table);

        assertThatSchemaContainsTable(table);
    }

    @Test
    @DisplayName("Создает таблицу с зависимостью на саму себя")
    void createTableWithSelfDependenciesTest() throws DatabaseException {
        Schema.resolve(ExchangeFolderReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("state", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "com.infomaximum.exchange")));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("email", "uuid"));
        }};
        Table table = new Table("ExchangeFolder", "com.infomaximum.exchange", fields, hashIndexes);
        schema.createTable(table);


        List<THashIndex> expectedHashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("parent_id"));
            add(new THashIndex("email", "uuid"));
        }};
        Table expected = new Table("ExchangeFolder", "com.infomaximum.exchange", fields, expectedHashIndexes);
        assertThatSchemaContainsTable(expected);
    }

    @Test
    @DisplayName("Создает таблицу с полями с зависимостями и всеми индексами")
    void createTableWithDependenciesAndIndexesTest() throws DatabaseException {
        createExchangeFolderTable();
        Schema.resolve(StoreFileReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("name", String.class));
            add(new TField("type", String.class));
            add(new TField("size", Long.class));
            add(new TField("single", Boolean.class));
            add(new TField("format", String.class));
            add(new TField("folder_id", new TableReference("ExchangeFolder", "com.infomaximum.exchange")));
            add(new TField("double", Double.class));
            add(new TField("begin_time", Instant.class));
            add(new TField("end_time", Instant.class));
            add(new TField("begin", Long.class));
            add(new TField("end", Long.class));
            add(new TField("local_begin", LocalDateTime.class));
            add(new TField("local_end", LocalDateTime.class));
            add(new TField("data", byte[].class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("size", "name"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        List<TPrefixIndex> prefixIndexes = new ArrayList<TPrefixIndex>() {{
            add(new TPrefixIndex("name"));
            add(new TPrefixIndex("name", "type"));
        }};
        List<TIntervalIndex> intervalIndexes = new ArrayList<TIntervalIndex>() {{
            add(new TIntervalIndex("size"));
            add(new TIntervalIndex("double"));
            add(new TIntervalIndex("begin_time"));
            add(new TIntervalIndex("local_begin"));
            add(new TIntervalIndex("size", new String[]{"name"}));
            add(new TIntervalIndex("size", new String[]{"folder_id"}));
        }};
        List<TRangeIndex> rangeIndexes = new ArrayList<TRangeIndex>() {{
            add(new TRangeIndex("begin", "end"));
            add(new TRangeIndex("begin", "end", new String[]{"folder_id"}));
            add(new TRangeIndex("begin_time", "end_time"));
            add(new TRangeIndex("begin_time", "begin_time", new String[]{"folder_id"}));
            add(new TRangeIndex("local_begin", "local_end"));
        }};
        Table table = new Table("StoreFile",
                "com.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        schema.createTable(table);

        hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("folder_id"));
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("name", "size"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        Table expected = new Table("StoreFile",
                "com.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        assertThatSchemaContainsTable(expected);
    }

    //Удаление таблицы_____________________________________________
    @Test
    @DisplayName("Удаляет простую таблицу")
    void removeTableTest() throws Exception {
        createExchangeFolderTable();
        createStoreFolderTable();

        schema.dropTable("StoreFile", "com.infomaximum.store");
        assertTableDoesntExist("StoreFile", "com.infomaximum.store");

        schema.dropTable("ExchangeFolder", "com.infomaximum.exchange");
        assertTableDoesntExist("ExchangeFolder", "com.infomaximum.exchange");
    }

    @Test
    @DisplayName("Ошибка при удалени таблицы, на которую ссылается другая таблица")
    void failBecauseRemoveDependencedTableTest() throws Exception {
        createExchangeFolderTable();
        createStoreFolderTable();

        Assertions.assertThatThrownBy(() -> schema.dropTable("ExchangeFolder", "com.infomaximum.exchange"))
                .isExactlyInstanceOf(TableRemoveException.class);
    }

    //Добавление полей_____________________________________________
    @Test
    @DisplayName("Добавляет поле в таблицу")
    void createTableFieldTest() throws Exception {
        Table generalTable = createGeneralTable();
        TField newField = new TField("newField", Long.class);
        schema.createField(newField, generalTable);

        List<TField> newFields = new ArrayList<>(generalTable.getFields());
        newFields.add(newField);
        generalTable = new Table(generalTable.getName(), generalTable.getNamespace(), newFields, generalTable.getHashIndexes());
        assertThatSchemaContainsTable(generalTable);
    }

    @Test
    @DisplayName("Ошибка добавления поля в таблицу, которое зависит от не существующей таблицы")
    void failCreateFieldBecauseForeignTableDoesntExist() throws Exception {
        createGeneralTable();
        TField newFieldWithDependence = new TField("newFieldWithDependence", new TableReference("ExchangeFolder",
                "com.infomaximum.exchange"));
        Assertions.assertThatThrownBy(() -> schema.createField(newFieldWithDependence, "general", "com.infomaximum.rocksdb"))
                .isExactlyInstanceOf(TableNotFoundException.class);
    }

    @Test
    @DisplayName("Ошибка имя поля уже существует")
    void failCreateFieldWithSameName() throws Exception {
        createGeneralTable();
        TField newFieldWithDependence = new TField("value", Double.class);
        Assertions.assertThatThrownBy(() -> schema.createField(newFieldWithDependence, "general", "com.infomaximum.rocksdb"))
                .isExactlyInstanceOf(FieldAlreadyExistsException.class);
    }

    //Удаление индексов_____________________________________________
    @Test
    @DisplayName("Удаляет hash index из таблицы с одним хэшем")
    void removeSimpleIndexTest() throws Exception {
        Table table = createGeneralTable();

        THashIndex removingHashIndex = new THashIndex("value");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(GeneralReadable.class, new HashFilter(GeneralEditable.FIELD_VALUE, 12L));
    }

    @Test
    @DisplayName("Удаляет index'ы из таблицы")
    void removeIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<StoreFileReadable> si = transaction.
                    find(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName")
                            .appendField(StoreFileReadable.FIELD_SIZE, 12L))) {
                Assertions.assertThat(si.hasNext()).isTrue();
            }
        });
    }

    @Test
    @DisplayName("Удаляет hash index, состоящий из нескольких полей, из таблицы")
    void removeMultiIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("size", "name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());

        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name").appendField(StoreFileReadable.FIELD_SIZE, "size"));
//        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "FileName"));
    }

    @Test
    @DisplayName("Удаляет hash index из таблицы и заного его добавляет")
    void removeAndAttachIndexTest() throws Exception {
        createExchangeFolderTable();
        Table table = createStoreFolderTable();

        THashIndex removingHashIndex = new THashIndex("name");
        schema.dropIndex(removingHashIndex, table.getName(), table.getNamespace());
        assertThatNoAnyRecords(StoreFileReadable.class, new HashFilter(StoreFileReadable.FIELD_FILE_NAME, "name"));

        schema.createIndex(removingHashIndex, "StoreFile", "com.infomaximum.store");
        List<THashIndex> expectedHashes = new ArrayList<>(table.getHashIndexes());
        expectedHashes.remove(removingHashIndex);
        expectedHashes.add(removingHashIndex);
        Table expected = new Table(table.getName(),
                table.getNamespace(),
                table.getFields(),
                expectedHashes,
                table.getPrefixIndexes(),
                table.getIntervalIndexes(),
                table.getRangeIndexes());
        assertThatSchemaContainsTable(expected);
    }

    //Переименование полей_____________________________________________
    @Test
    @DisplayName("Переименование поля таблицы")
    void renameTableFieldTest() throws DatabaseException {
        Table generalTable = createExchangeFolderTable();
        schema.renameField("state", "newValue", "ExchangeFolder", "com.infomaximum.exchange");

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("newValue", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "com.infomaximum.exchange")));
        }};
        generalTable = new Table(generalTable.getName(), generalTable.getNamespace(), fields, generalTable.getHashIndexes());
        assertThatSchemaContainsTable(generalTable);
    }

    @Test
    @DisplayName("Ошибка переименования поля таблицы. Поле с таким именем уже существует")
    void failRenameTableFieldNameAlreadyExistsTest() throws DatabaseException {
        createExchangeFolderTable();
        Assertions.assertThatThrownBy(() -> schema.renameField("state", "email", "ExchangeFolder", "com.infomaximum.exchange"))
                .isExactlyInstanceOf(FieldAlreadyExistsException.class);
    }

    private Table createGeneralTable() throws Exception {
        Schema.resolve(GeneralReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("value", Long.class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("value"));
        }};
        Table table = new Table("general", "com.infomaximum.rocksdb", fields, hashIndexes);
        schema.createTable(table);
        domainObjectSource.executeTransactional(transaction -> {
            GeneralEditable generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(12L);
            transaction.save(generalEditable);

            generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(111L);
            transaction.save(generalEditable);

            generalEditable = transaction.create(GeneralEditable.class);
            generalEditable.setValue(12L);
            transaction.save(generalEditable);
        });
        return schema.getTable("general", "com.infomaximum.rocksdb");
    }

    private Table createExchangeFolderTable() throws DatabaseException {
        Schema.resolve(ExchangeFolderReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("uuid", String.class));
            add(new TField("email", String.class));
            add(new TField("date", Instant.class));
            add(new TField("state", String.class));
            add(new TField("parent_id", new TableReference("ExchangeFolder", "com.infomaximum.exchange")));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("email", "uuid"));
        }};
        Table table = new Table("ExchangeFolder", "com.infomaximum.exchange", fields, hashIndexes);
        schema.createTable(table);
        return schema.getTable("ExchangeFolder", "com.infomaximum.exchange");
    }

    private Table createStoreFolderTable() throws Exception {
        Schema.resolve(StoreFileReadable.class);

        List<TField> fields = new ArrayList<TField>() {{
            add(new TField("name", String.class));
            add(new TField("type", String.class));
            add(new TField("size", Long.class));
            add(new TField("single", Boolean.class));
            add(new TField("format", String.class));
            add(new TField("folder_id", new TableReference("ExchangeFolder", "com.infomaximum.exchange")));
            add(new TField("double", Double.class));
            add(new TField("begin_time", Instant.class));
            add(new TField("end_time", Instant.class));
            add(new TField("begin", Long.class));
            add(new TField("end", Long.class));
            add(new TField("local_begin", LocalDateTime.class));
            add(new TField("local_end", LocalDateTime.class));
            add(new TField("data", byte[].class));
        }};
        List<THashIndex> hashIndexes = new ArrayList<THashIndex>() {{
            add(new THashIndex("size"));
            add(new THashIndex("name"));
            add(new THashIndex("size", "name"));
            add(new THashIndex("format"));
            add(new THashIndex("local_begin"));
        }};
        List<TPrefixIndex> prefixIndexes = new ArrayList<TPrefixIndex>() {{
            add(new TPrefixIndex("name"));
            add(new TPrefixIndex("name", "type"));
        }};
        List<TIntervalIndex> intervalIndexes = new ArrayList<TIntervalIndex>() {{
            add(new TIntervalIndex("size"));
            add(new TIntervalIndex("double"));
            add(new TIntervalIndex("begin_time"));
            add(new TIntervalIndex("local_begin"));
            add(new TIntervalIndex("size", new String[]{"name"}));
            add(new TIntervalIndex("size", new String[]{"folder_id"}));
        }};
        List<TRangeIndex> rangeIndexes = new ArrayList<TRangeIndex>() {{
            add(new TRangeIndex("begin", "end"));
            add(new TRangeIndex("begin", "end", new String[]{"folder_id"}));
            add(new TRangeIndex("begin_time", "end_time"));
            add(new TRangeIndex("begin_time", "begin_time", new String[]{"folder_id"}));
            add(new TRangeIndex("local_begin", "local_end"));
        }};
        Table table = new Table("StoreFile",
                "com.infomaximum.store",
                fields,
                hashIndexes,
                prefixIndexes,
                intervalIndexes,
                rangeIndexes
        );
        schema.createTable(table);

        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(12L);
            storeFileEditable.setFileName("FileName");
            storeFileEditable.setDouble(12.4);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(11L);
            storeFileEditable.setFileName("FileName2");
            storeFileEditable.setDouble(13.4);
            transaction.save(storeFileEditable);

            storeFileEditable = transaction.create(StoreFileEditable.class);
            storeFileEditable.setSize(1L);
            storeFileEditable.setFileName("FileName");
            storeFileEditable.setDouble(13.123);
            transaction.save(storeFileEditable);
        });
        return schema.getTable("StoreFile", "com.infomaximum.store");
    }

    private void assertThatSchemaContainsTable(Table expected) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        Table actual = schema.getTable(expected.getName(), expected.getNamespace());
        Assertions.assertThat(actual).isEqualTo(expected);
        String dataColumnFamily = expected.getNamespace() + "." + expected.getName();
        String indexColumnFamily = expected.getNamespace() + "." + expected.getName() + ".index";
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(dataColumnFamily)).isTrue();
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(indexColumnFamily)).isTrue();
    }

    private void assertTableDoesntExist(String name, String namespace) throws DatabaseException {
        Schema schema = Schema.read(rocksDBProvider);
        Assertions.assertThatThrownBy(() -> schema.getTable(name, namespace)).isExactlyInstanceOf(TableNotFoundException.class);
        String dataColumnFamily = namespace + "." + name;
        String indexColumnFamily = namespace + "." + name + ".index";
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(dataColumnFamily)).isFalse();
        Assertions.assertThat(rocksDBProvider.containsColumnFamily(indexColumnFamily)).isFalse();
    }

    private <T extends DomainObject> void assertThatNoAnyRecords(Class<T> dbEntity, Filter filter) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            try(IteratorEntity<T> it = transaction.find(dbEntity, filter)) {
                Assertions.assertThat(it.hasNext()).isFalse();
            }
        });
    }
}
