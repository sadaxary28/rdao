package com.infomaximum.database.domainobject;

import com.infomaximum.database.exception.runtime.StructEntityException;
import com.infomaximum.database.schema.BaseIndex;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.BoundaryEditable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.legacy.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BaseIndex.class)
public class DomainTest extends DomainDataTest{

    @Test(expected = StructEntityException.class)
    public void failBecauseSameFieldsHash() throws Exception {
        PowerMockito.spy(BaseIndex.class);
        PowerMockito.when(BaseIndex.class, "buildFieldsHashCRC32", ArgumentMatchers.anyList())
                .then(h -> TypeConvert.pack(1));

        createDomain(BoundaryEditable.class);
    }
}