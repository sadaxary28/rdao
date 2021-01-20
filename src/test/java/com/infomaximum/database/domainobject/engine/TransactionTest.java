package com.infomaximum.database.domainobject.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.domainobject.filter.EmptyFilter;
import com.infomaximum.database.domainobject.iterator.IteratorEntity;
import com.infomaximum.database.exception.ForeignDependencyException;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.DomainService;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.*;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;


public class TransactionTest extends StoreFileDataTest {

    @Test
    public void optimisticTransactionLazyTest() throws Exception {
        String fileName = "aaa.txt";
        long size = 15L;

        recordSource.executeTransactional(transaction -> {
            long id = transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"name", "size"}, new Object[] {fileName, size});

            try (RecordIterator ie = transaction.select(STORE_FILE_NAME, STORE_FILE_NAMESPACE)) {
                Record storeFile = ie.next();

                Assert.assertEquals(fileName, storeFile.getValues()[StoreFileReadable.FIELD_FILE_NAME]);
                Assert.assertEquals(size, storeFile.getValues()[StoreFileReadable.FIELD_SIZE]);
            }
        });
    }

    @Test
    public void create() throws Exception {
        //Проверяем, что такого объекта нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));

        String fileName="application/json";
        String contentType="info.json";
        long size=1000L;

        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{"name", "size", "type"}, new Object[] {fileName, size, contentType});
        });

        //Загружаем сохраненый объект
        Record storeFileCheckSave = recordSource.executeFunctionTransactional(transaction -> transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1));
        Assertions.assertThat(storeFileCheckSave).isNotNull();
        Assertions.assertThat(fileName).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_FILE_NAME]);
        Assertions.assertThat(contentType).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_CONTENT_TYPE]);
        Assertions.assertThat(size).isEqualTo(storeFileCheckSave.getValues()[StoreFileReadable.FIELD_SIZE]);
    }

    @Test
    public void update() throws Exception {
        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord(FOLDER_FILE_NAME, FOLDER_FILE_NAMESPACE,
                    new String[]{}, new Object[]{});
        });
        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    new String[]{}, new Object[]{});
        });
        Record obj = recordSource.executeFunctionTransactional(transaction -> transaction.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, 1));
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()).containsOnlyNulls();

        //Добавляем объект
        long recordId = recordSource.executeFunctionTransactional(transaction -> transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                new String[]{"name", "folder_id", "single", "data", "double"},
                new Object[]{"test", 1L, false, new byte[]{1,2}, 0.1}));

        //Загружаем сохраненый объект
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo("test");
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FOLDER_ID]).isEqualTo(1L);
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_SINGLE]).isEqualTo(false);
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isEqualTo(new byte[]{1,2});
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DOUBLE]).isEqualTo(0.1);

        //Редактируем сохраненный объект
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    recordId,
                    new String[]{"name", "folder_id", "single", "data", "double"},
                    new Object[]{null, null, null, null, null});
        });
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FOLDER_ID]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_SINGLE]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DOUBLE]).isNull();

        //Повторно редактируем сохраненный объект
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                    recordId,
                    new String[]{"name", "data"},
                    new Object[]{"", new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2}});
        });
        obj = recordSource.getById(STORE_FILE_NAME, STORE_FILE_NAMESPACE, recordId);
        Assertions.assertThat(obj).isNotNull();
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_FILE_NAME]).isEqualTo("");
        Assertions.assertThat(obj.getValues()[StoreFileReadable.FIELD_DATA]).isEqualTo(new byte[] {TypeConvert.NULL_BYTE_ARRAY_SCHIELD, 1, 2});
    }

    @Test
    public void updateByNonExistenceObject() {
        Assertions.assertThatThrownBy(() ->
                recordSource.executeTransactional(transaction -> {
                    transaction.insertRecord(STORE_FILE_NAME, STORE_FILE_NAMESPACE,
                            new String[]{"folder_id"}, new Object[] {256L});
        })).isInstanceOf(ForeignDependencyException.class);
    }

    @Test
    public void updateValueStringEmptyThenNull() throws Exception {
        final long objectId = 1;
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(emptyFileName);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckSave = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckSave);
        Assert.assertEquals(emptyFileName, storeFileCheckSave.getFileName());
        Assert.assertEquals(contentType, storeFileCheckSave.getContentType());

        //Редактируем сохраненый объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = domainObjectSource.get(StoreFileEditable.class, objectId);
            obj.setContentType(null);
            transaction.save(obj);
        });

        //Загружаем сохраненый объект
        StoreFileReadable storeFileCheckEdit = domainObjectSource.get(StoreFileReadable.class, objectId);
        Assert.assertNotNull(storeFileCheckEdit);
        Assert.assertEquals(emptyFileName, storeFileCheckEdit.getFileName());
        Assert.assertNull(storeFileCheckEdit.getContentType());
    }

    @Test
    public void saveEmptyDomainObject() throws Exception {
        final long objectId = 1;
        final String emptyFileName = "";
        final String contentType = "info.json";

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable storeFile = transaction.create(StoreFileEditable.class);
            storeFile.setContentType(contentType);
            storeFile.setFileName(emptyFileName);
            transaction.save(storeFile);
            transaction.save(storeFile);
        });

        //Загружаем сохраненый объект и сразу без редактирования полей вызываем сохранение
        domainObjectSource.executeTransactional(transaction -> {
            StoreFileEditable obj = transaction.get(StoreFileEditable.class, objectId);
            transaction.save(obj);
        });
    }

    @Test
    public void removeOneObject() throws Exception {
        //Проверяем, что таких объектов нет в базе
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 3L));

        //Добавляем объект
        domainObjectSource.executeTransactional(transaction -> {
            transaction.save(transaction.create(StoreFileEditable.class));
            transaction.save(transaction.create(StoreFileEditable.class));
            transaction.save(transaction.create(StoreFileEditable.class));
        });

        //Проверяем что файлы сохранены
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));

        //Удяляем 2-й объект
        domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(StoreFileEditable.class, 2L)));

        //Проверяем, корректность удаления
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 1L));
        Assert.assertNull(domainObjectSource.get(StoreFileReadable.class, 2L));
        Assert.assertNotNull(domainObjectSource.get(StoreFileReadable.class, 3L));
    }

    @Test
    public void removeReferencedObject() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);
        });

        try {
            domainObjectSource.executeTransactional(transaction -> transaction.remove(transaction.get(ExchangeFolderEditable.class, 1)));
            Assert.fail();
        } catch (ForeignDependencyException ex) {
            try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertTrue(i.hasNext());
            }
        }

        domainObjectSource.executeTransactional(transaction -> {
            transaction.remove(transaction.get(StoreFileEditable.class, 1));
            transaction.remove(transaction.get(ExchangeFolderEditable.class, 1));
        });
    }

    @Test
    public void removeAll() throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);
        });

        domainObjectSource.executeTransactional(transaction -> transaction.removeAll(ExchangeFolderEditable.class));
        try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
            Assert.assertFalse(i.hasNext());
        }
        new DomainService(domainObjectSource.getDbProvider(), schema)
                .setChangeMode(ChangeMode.NONE)
                .setValidationMode(true)
                .setDomain(Schema.getEntity(ExchangeFolderEditable.class))
                .execute();

        domainObjectSource.executeTransactional(transaction -> {
            ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
            transaction.save(folder);

            StoreFileEditable file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(folder.getId());
            transaction.save(file);

            file = transaction.create(StoreFileEditable.class);
            file.setFolderId(null);
            transaction.save(file);
        });

        try {
            domainObjectSource.executeTransactional(transaction -> transaction.removeAll(ExchangeFolderEditable.class));
            Assert.fail();
        } catch (ForeignDependencyException e) {
            try (IteratorEntity<ExchangeFolderReadable> i = domainObjectSource.find(ExchangeFolderReadable.class, EmptyFilter.INSTANCE)) {
                Assert.assertTrue(i.hasNext());
            }
        }

        domainObjectSource.executeTransactional(transaction -> {
            transaction.removeAll(StoreFileEditable.class);
            transaction.removeAll(ExchangeFolderEditable.class);
        });
    }
}
