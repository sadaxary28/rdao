package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.SchemaException;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldUtil;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class DBRangeIndex extends DBIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("rng");
    private static final String JSON_PROP_BEGIN_FIELD_ID = "begin_field_id";
    private static final String JSON_PROP_END_FIELD_ID = "end_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final DBField beginField;
    private final DBField endField;
    private final List<DBField> hashFields;

    private DBRangeIndex(int id, DBField beginField, DBField endField, List<DBField> hashFields) {
        super(id, concatenate(beginField, endField, hashFields));
        checkSorting(hashFields);

        this.beginField = beginField;
        this.endField = endField;
        this.hashFields = hashFields;
    }

    public DBRangeIndex(DBField beginField, DBField endField, List<DBField> hashFields) {
        this(-1, beginField, endField, hashFields);
    }

    public DBField getBeginField() {
        return beginField;
    }

    public DBField getEndField() {
        return endField;
    }

    public List<DBField> getHashFields() {
        return hashFields;
    }

    @Override
    protected byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }

    static DBRangeIndex fromJson(JSONObject source, List<DBField> allFields) throws SchemaException {
        int beginFieldId = JsonUtils.getValue(JSON_PROP_BEGIN_FIELD_ID, Integer.class, source);
        DBField beginField = FieldUtil.getFieldsById(beginFieldId, allFields);
        int endFieldId = JsonUtils.getValue(JSON_PROP_END_FIELD_ID, Integer.class, source);
        DBField endField = FieldUtil.getFieldsById(endFieldId, allFields);
        int[] hashFieldIds = JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source);
        List<DBField> hashFields = FieldUtil.getFieldsByIds(hashFieldIds, allFields);
        return new DBRangeIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                beginField,
                endField,
                hashFields
        );
    }

    @Override
    JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put(JSON_PROP_ID, getId());
        object.put(JSON_PROP_BEGIN_FIELD_ID, beginField);
        object.put(JSON_PROP_END_FIELD_ID, endField);
        object.put(JSON_PROP_HASH_FIELD_IDS, JsonUtils.toJsonArray(hashFields));
        return object;
    }

    private static List<DBField> concatenate(DBField beginField, DBField endField, List<DBField> hashFields) {
        List<DBField> result = new ArrayList<>(hashFields.size() + 2);
        result.addAll(hashFields);
        result.add(beginField);
        result.add(endField);
        return result;
    }
}
