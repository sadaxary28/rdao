package com.infomaximum.database.schema;

import com.infomaximum.database.exception.runtime.StructEntityException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.domain.BoundaryReadable;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
//
//@ExtendWith(PowerMockExtension.class)
//@PrepareForTest(BaseIndex.class)
//public class StructEntityTest {
//
//    @Test(expected = StructEntityException.class)
//    public void createThenFailBecauseSameFieldsHash() throws Exception {
//        PowerMockito.spy(BaseIndex.class);
//        PowerMockito.when(BaseIndex.class, "buildFieldsHashCRC32", ArgumentMatchers.anyList())
//                .thenReturn(TypeConvert.pack(1));
//
//        new StructEntity(BoundaryReadable.class);
//    }
//}
