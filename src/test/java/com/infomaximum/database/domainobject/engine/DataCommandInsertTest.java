package com.infomaximum.database.domainobject.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.database.utils.TableUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataCommandInsertTest extends StoreFileDataTest {

    @Test
    public void insertSimple() throws Exception {
        String[] fields = new String[]{"name"};
        Object[] values = new Object[]{"objName"};
        long id = recordSource.executeTransactional(dataCommand -> {
            return dataCommand.insertRecord("StoreFile", "com.infomaximum.store", fields, values);
        });
        assertThatDBContainsRecord(id, fields, values, "StoreFile", "com.infomaximum.store");
    }

    private void assertThatDBContainsRecord(long id, String[] fields, Object[] values, String table, String namespace) {
        try (RecordIterator i = recordSource.select(table, namespace)) {
            while (i.hasNext()) {
                Record record = i.next();
                if (record.getId() == id) {
                    Object[] actualValues = TableUtils.sortValuesByFieldOrder(table, namespace, fields, values, schema.getDbSchema());
                    Assertions.assertThat(record.getValues()).isEqualTo(actualValues);
                    return;
                }
            }
        }
        Assertions.fail("БД не содержит заданный record");
    }
}