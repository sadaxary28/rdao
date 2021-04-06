package com.infomaximum.database.domainobject.engine;

import com.infomaximum.database.RecordSource;
import com.infomaximum.database.domainobject.StoreFileDataTest;
import com.infomaximum.domain.SelfDependencyReadable;
import org.junit.jupiter.api.Test;


public class SelfDependencyTest extends StoreFileDataTest {

    @Test
    public void dependencyOnOtherObject() throws Exception {
        createDomain(SelfDependencyReadable.class);
        recordSource = new RecordSource(rocksDBProvider);

        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord("SelfDependency", "com.infomaximum.self", new String[] {}, new Object[] {}));
        recordSource.executeTransactional(transaction -> {
            transaction.insertRecord("SelfDependency", "com.infomaximum.self", new Object[] {recordId});
        });
    }
    
    @Test
    public void updateDependencyOnTheObject() throws Exception {
        createDomain(SelfDependencyReadable.class);
        recordSource = new RecordSource(rocksDBProvider);

        long recordId = recordSource.executeFunctionTransactional(transaction ->
                transaction.insertRecord("SelfDependency", "com.infomaximum.self", new String[] {}, new Object[] {}));
        recordSource.executeTransactional(transaction -> {
            transaction.updateRecord("SelfDependency", "com.infomaximum.self", recordId, new String[] {"dependence"}, new Object[] {recordId});
        });
    }
}
