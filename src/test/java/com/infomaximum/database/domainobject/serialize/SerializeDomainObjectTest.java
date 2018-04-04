package com.infomaximum.database.domainobject.serialize;

import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.domain.ExchangeFolderEditable;
import com.infomaximum.domain.StoreFileEditable;
import com.infomaximum.domain.type.FormatType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by kris on 22.04.17.
 */
public class SerializeDomainObjectTest extends StoreFileDataTest {

    @Test
    public void run() throws Exception {

        StoreFileEditable storeFile = new StoreFileEditable(1);
        storeFile.setContentType("application/json");
        storeFile.setFileName("info.json");
        storeFile.setSize(1000L);
        storeFile.setFormat(FormatType.B);
        testSerialize(storeFile);


        ExchangeFolderEditable exchangeFolder = new ExchangeFolderEditable(1);
        exchangeFolder.setUuid("1111");
        exchangeFolder.setUserEmail("2222@sgsdfg.com");
        exchangeFolder.setSyncDate(null);
        exchangeFolder.setSyncState("555");
        exchangeFolder.setParentId(10L);
        testSerialize(exchangeFolder);

    }


    private static void testSerialize(Serializable object) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(object);
                baos.toByteArray();
            }
        }
    }
}
