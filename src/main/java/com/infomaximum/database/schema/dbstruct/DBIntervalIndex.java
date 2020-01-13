package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldUtil;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class DBIntervalIndex extends DBIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("int");
    private static final String JSON_PROP_INDEXED_FIELD_ID = "indexed_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final DBField indexedField;
    private final List<DBField> hashFields;

    private DBIntervalIndex(int id, DBField indexedField, List<DBField> hashFields) {
        super(id, concatenate(indexedField, hashFields));
        checkSorting(hashFields);

        this.indexedField = indexedField;
        this.hashFields = hashFields;
    }

    public DBIntervalIndex(DBField indexedField, List<DBField> hashFields) {
        this(-1, indexedField, hashFields);
    }

    public DBField getIndexedField() {
        return indexedField;
    }

    public List<DBField> getHashFields() {
        return hashFields;
    }

    @Override
    protected byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }

    static DBIntervalIndex fromJson(JSONObject source, List<DBField> allFields) throws SchemaException {
        int indexedFieldId = JsonUtils.getValue(JSON_PROP_INDEXED_FIELD_ID, Integer.class, source);
        DBField indexedField = FieldUtil.getFieldsById(indexedFieldId, allFields);
        int[] hashFieldIds = JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source);
        List<DBField> hashFields = FieldUtil.getFieldsByIds(hashFieldIds, allFields);
        return new DBIntervalIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                indexedField,
                hashFields
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_INDEXED_FIELD_ID, indexedField);
        object.put(JSON_PROP_HASH_FIELD_IDS, JsonUtils.toJsonArray(hashFields));
        return object;
    }

    private static List<DBField> concatenate(DBField indexedField, List<DBField> hashFields) {
        List<DBField> result = new ArrayList<>(hashFields.size() + 1);
        result.addAll(hashFields);
        result.add(indexedField);
        return result;
    }
}
