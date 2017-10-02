package com.infomaximum.database.domainobject.utils;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.utils.IndexUtils;

import java.util.List;
import java.util.Map;

public class DomainIndexUtils {

    public static void setHashValues(final List<EntityField> sortedFields, final Map<EntityField, Object> values, long[] destination) {
        for (int i = 0; i < sortedFields.size(); ++i) {
            EntityField field = sortedFields.get(i);
            destination[i] = IndexUtils.buildHash(field.getType(), values.get(field));
        }
    }
}
