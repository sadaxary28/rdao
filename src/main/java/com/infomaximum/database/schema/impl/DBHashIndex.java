package com.infomaximum.database.schema.impl;

import com.infomaximum.database.exception.SchemaException;
import net.minidev.json.JSONObject;

public class DBHashIndex extends DBIndex {

    private static final String JSON_PROP_FIELD_IDS = "field_ids";

    private DBHashIndex(int id, int[] fieldIds) {
        super(id, fieldIds);
        checkSorting(fieldIds);
    }

    public DBHashIndex(int[] fieldIds) {
        this(-1, fieldIds);
    }

    static DBHashIndex fromJson(JSONObject source) throws SchemaException {
        return new DBHashIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                JsonUtils.getIntArrayValue(JSON_PROP_FIELD_IDS, source)
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_FIELD_IDS, JsonUtils.toJsonArray(getFieldIds()));
        return object;
    }
}
