package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import net.minidev.json.JSONObject;

import java.util.Arrays;

public class DBRangeIndex extends DBIndex {

    private static final String JSON_PROP_BEGIN_FIELD_ID = "begin_field_id";
    private static final String JSON_PROP_END_FIELD_ID = "end_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final int beginFieldId;
    private final int endFieldId;
    private final int[] hashFieldIds;

    private DBRangeIndex(int id, int beginFieldId, int endFieldId, int[] hashFieldIds) {
        super(id, concatenate(beginFieldId, endFieldId, hashFieldIds));
        checkSorting(hashFieldIds);

        this.beginFieldId = beginFieldId;
        this.endFieldId = endFieldId;
        this.hashFieldIds = hashFieldIds;
    }

    public DBRangeIndex(int beginFieldId, int endFieldId, int[] hashFieldIds) {
        this(-1, beginFieldId, endFieldId, hashFieldIds);
    }

    public int getBeginFieldId() {
        return beginFieldId;
    }

    public int getEndFieldId() {
        return endFieldId;
    }

    public int[] getHashFieldIds() {
        return hashFieldIds;
    }

    static DBRangeIndex fromJson(JSONObject source) throws SchemaException {
        return new DBRangeIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                JsonUtils.getValue(JSON_PROP_BEGIN_FIELD_ID, Integer.class, source),
                JsonUtils.getValue(JSON_PROP_END_FIELD_ID, Integer.class, source),
                JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source)
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_BEGIN_FIELD_ID, beginFieldId);
        object.put(JSON_PROP_END_FIELD_ID, endFieldId);
        object.put(JSON_PROP_HASH_FIELD_IDS, JsonUtils.toJsonArray(hashFieldIds));
        return object;
    }

    private static int[] concatenate(int beginFieldId, int endFieldId, int[] hashFieldIds) {
        int[] fieldIds = Arrays.copyOf(hashFieldIds, hashFieldIds.length + 2);
        fieldIds[fieldIds.length - 2] = beginFieldId;
        fieldIds[fieldIds.length - 1] = endFieldId;
        return fieldIds;
    }
}
