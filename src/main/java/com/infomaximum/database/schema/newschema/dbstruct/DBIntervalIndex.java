package com.infomaximum.database.schema.newschema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import net.minidev.json.JSONObject;

import java.util.Arrays;

public class DBIntervalIndex extends DBIndex {

    private static final String JSON_PROP_INDEXED_FIELD_ID = "indexed_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final int indexedFieldId;
    private final int[] hashFieldIds;

    private DBIntervalIndex(int id, int indexedFieldId, int[] hashFieldIds) {
        super(id, concatenate(indexedFieldId, hashFieldIds));
        checkSorting(hashFieldIds);

        this.indexedFieldId = indexedFieldId;
        this.hashFieldIds = hashFieldIds;
    }

    public DBIntervalIndex(int indexedFieldId, int[] hashFieldIds) {
        this(-1, indexedFieldId, hashFieldIds);
    }

    public int getIndexedFieldId() {
        return indexedFieldId;
    }

    public int[] getHashFieldIds() {
        return hashFieldIds;
    }

    static DBIntervalIndex fromJson(JSONObject source) throws SchemaException {
        return new DBIntervalIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                JsonUtils.getValue(JSON_PROP_INDEXED_FIELD_ID, Integer.class, source),
                JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source)
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_INDEXED_FIELD_ID, indexedFieldId);
        object.put(JSON_PROP_HASH_FIELD_IDS, JsonUtils.toJsonArray(hashFieldIds));
        return object;
    }

    private static int[] concatenate(int indexedFieldId, int[] hashFieldIds) {
        int[] fieldIds = Arrays.copyOf(hashFieldIds, hashFieldIds.length + 1);
        fieldIds[fieldIds.length - 1] = indexedFieldId;
        return fieldIds;
    }
}
