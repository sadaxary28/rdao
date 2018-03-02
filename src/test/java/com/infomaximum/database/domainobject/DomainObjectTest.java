package com.infomaximum.database.domainobject;

import com.infomaximum.database.schema.EntityField;
import com.infomaximum.database.schema.StructEntity;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.StoreFileReadable;
import com.infomaximum.domain.type.FormatType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DomainObjectTest extends StoreFileDataTest {

    @Test
    public void getStructEntity() throws Exception {
        initAndFillStoreFiles(domainObjectSource, 10);

        StoreFileReadable storeFile = domainObjectSource.get(StoreFileReadable.class, 1, Collections.singleton(StoreFileReadable.FIELD_FILE_NAME));

        StructEntity structEntity = storeFile.getStructEntity();
        Map<String, Object> fields = new HashMap<>(structEntity.getFields().size());
        for (EntityField field : structEntity.getFields()) {
            fields.put(field.getName(), storeFile.get(field));
        }

        Assert.assertEquals("name", fields.get(StoreFileReadable.FIELD_FILE_NAME));
        Assert.assertEquals(true, fields.get(StoreFileReadable.FIELD_SINGLE));
        Assert.assertEquals(FormatType.B, fields.get(StoreFileReadable.FIELD_FORMAT));
    }

    private void initAndFillStoreFiles(DomainObjectSource domainObjectSource, int recordCount) throws Exception {
        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; i++) {
                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setSize(10);
                obj.setFileName("name");
                obj.setContentType("type");
                obj.setSingle(true);
                obj.setFormat(FormatType.B);
                transaction.save(obj);
            }
        });
    }
}
