package com.infomaximum.database.utils.key;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.schema.dbstruct.DBField;

import java.util.ArrayList;
import java.util.List;

public class FieldUtil {

    public static List<DBField> getFieldsByIds(int[] fieldIds, List<DBField> allFields) {
        List<DBField> result = new ArrayList<>(fieldIds.length);
        for (int fieldId : fieldIds) {
            result.add(getFieldsById(fieldId, allFields));
        }
        return result;
    }

    public static DBField getFieldsById(int fieldId, List<DBField> allFields) {
        return allFields.stream()
                    .filter(dbField -> dbField.getId() == fieldId)
                    .findAny()
                    .orElseThrow(() -> new SchemaException("Can't find field for index with id: " + fieldId));
    }
}
