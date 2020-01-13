package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldUtil;
import net.minidev.json.JSONObject;

import java.util.List;

public class DBHashIndex extends DBIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("hsh");
    private static final String JSON_PROP_FIELD_IDS = "field_ids";

    private DBHashIndex(int id, List<DBField> sortedFields) {
        super(id, sortedFields);
        checkSorting(sortedFields);
    }

    public DBHashIndex(List<DBField> sortedFields) {
        this(-1, sortedFields);
    }

    @Override
    protected byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }

    static DBHashIndex fromJson(JSONObject source, List<DBField> allFields) throws SchemaException {
        int[] fieldIds = JsonUtils.getIntArrayValue(JSON_PROP_FIELD_IDS, source);
        List<DBField> fields = FieldUtil.getFieldsByIds(fieldIds, allFields);
        return new DBHashIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                fields);
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_FIELD_IDS, JsonUtils.toJsonArray(getFields()));
        return object;
    }
}
