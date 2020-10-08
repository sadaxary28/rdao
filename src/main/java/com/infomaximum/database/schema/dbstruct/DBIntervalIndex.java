package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import net.minidev.json.JSONObject;

import java.util.Arrays;
import java.util.List;

public class DBIntervalIndex extends DBIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("int");
    private static final String JSON_PROP_INDEXED_FIELD_ID = "indexed_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final int indexedFieldId;
    private final int[] hashFieldIds;


    DBIntervalIndex(int id, DBField indexedFieldId, DBField[] hashFieldIds) {
        super(id, concatenate(indexedFieldId, hashFieldIds));
        checkSorting(hashFieldIds);

        this.indexedFieldId = indexedFieldId.getId();
        this.hashFieldIds = Arrays.stream(hashFieldIds).mapToInt(DBObject::getId).toArray();
    }

    public DBIntervalIndex(DBField indexedFieldId, DBField[] hashFieldIds) {
        this(-1, indexedFieldId, hashFieldIds);
    }

    public int getIndexedFieldId() {
        return indexedFieldId;
    }

    public int[] getHashFieldIds() {
        return hashFieldIds;
    }

    @Override
    protected byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }

    static DBIntervalIndex fromJson(JSONObject source, List<DBField> tableFields) throws SchemaException {
        return new DBIntervalIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                IndexUtils.getFieldsByIds(tableFields, JsonUtils.getValue(JSON_PROP_INDEXED_FIELD_ID, Integer.class, source)),
                IndexUtils.getFieldsByIds(tableFields, JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source))
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

    private static DBField[] concatenate(DBField indexedFieldId, DBField[] hashFieldIds) {
        DBField[] fieldIds = Arrays.copyOf(hashFieldIds, hashFieldIds.length + 1);
        fieldIds[fieldIds.length - 1] = indexedFieldId;
        return fieldIds;
    }
}
