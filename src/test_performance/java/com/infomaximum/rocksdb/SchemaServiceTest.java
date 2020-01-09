package com.infomaximum.rocksdb;

import com.infomaximum.database.domainobject.DomainDataTest;
import com.infomaximum.database.maintenance.ChangeMode;
import com.infomaximum.database.maintenance.SchemaService;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.domain.ExchangeFolderEditable;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.rocksdb.util.PerfomanceTest;
import org.junit.Test;

public class SchemaServiceTest extends DomainDataTest {

    @Test
    public void validateCoherentData() throws Exception {
        createDomain(StoreFileEditable.class);
        createDomain(ExchangeFolderEditable.class);

        final int recordCount = 100 * 1000;

        domainObjectSource.executeTransactional(transaction -> {
            for (int i = 0; i < recordCount; ++i) {
                ExchangeFolderEditable folder = transaction.create(ExchangeFolderEditable.class);
                transaction.save(folder);

                StoreFileEditable obj = transaction.create(StoreFileEditable.class);
                obj.setFolderId(folder.getId());
                transaction.save(obj);
            }
        });

        SchemaService schemaService = new SchemaService(rocksDBProvider)
                .setNamespace("com.infomaximum.store")
                .setChangeMode(ChangeMode.NONE)
                .setSchema(Schema.read(rocksDBProvider));

        PerfomanceTest.test(10, step -> {
            schemaService.execute();
        });
    }
}
