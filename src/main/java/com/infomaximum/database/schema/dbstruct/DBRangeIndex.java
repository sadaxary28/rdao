package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.exception.runtime.SchemaException;
import com.infomaximum.database.utils.IndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import net.minidev.json.JSONObject;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class DBRangeIndex extends DBIndex {

    private final static byte[] INDEX_NAME_BYTES = TypeConvert.pack("rng");
    private static final String JSON_PROP_BEGIN_FIELD_ID = "begin_field_id";
    private static final String JSON_PROP_END_FIELD_ID = "end_field_id";
    private static final String JSON_PROP_HASH_FIELD_IDS = "hash_field_ids";

    private final int beginFieldId;
    private final int endFieldId;
    private final int[] hashFieldIds;

    private final DBField[] tempHashFields; //todo V.Bukharkin remove it
    private final DBField tempBeginField; //todo V.Bukharkin remove it
    private final DBField tempEndField; //todo V.Bukharkin remove it

    private DBRangeIndex(int id, DBField beginField, DBField endField, DBField[] hashFields) {
        super(id, concatenate(beginField, endField, hashFields));
        checkSorting(hashFields);

        this.beginFieldId = beginField.getId();
        this.endFieldId = endField.getId();
        this.hashFieldIds = Arrays.stream(hashFields).mapToInt(DBField::getId).toArray();
        tempHashFields = hashFields;
        tempBeginField = beginField;
        tempEndField = endField;
    }

    public DBRangeIndex(DBField beginField, DBField endField, DBField[] hashFields) {
        this(-1, beginField, endField, hashFields);
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

    @Override
    protected byte[] getIndexNameBytes() {
        return INDEX_NAME_BYTES;
    }

    static DBRangeIndex fromJson(JSONObject source, List<DBField> tableFields) throws SchemaException {
        return new DBRangeIndex(
                JsonUtils.getValue(JSON_PROP_ID, Integer.class, source),
                IndexUtils.getFieldsByIds(tableFields, JsonUtils.getValue(JSON_PROP_BEGIN_FIELD_ID, Integer.class, source)),
                IndexUtils.getFieldsByIds(tableFields, JsonUtils.getValue(JSON_PROP_END_FIELD_ID, Integer.class, source)),
                IndexUtils.getFieldsByIds(tableFields, JsonUtils.getIntArrayValue(JSON_PROP_HASH_FIELD_IDS, source))
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

    private static DBField[] concatenate(DBField beginFieldId, DBField endFieldId, DBField[] hashFieldIds) {
        DBField[] fieldIds = Arrays.copyOf(hashFieldIds, hashFieldIds.length + 2);
        fieldIds[fieldIds.length - 2] = beginFieldId;
        fieldIds[fieldIds.length - 1] = endFieldId;
        return fieldIds;
    }

    public DBRangeIndex getTempFixed() {
        if (getId() < 1) {
            throw new RuntimeException("Bad error");
        }
        setId(getId() - 1);
        Arrays.sort(tempHashFields, Comparator.comparing(DBField::getName));
        return new DBRangeIndex(getId(), tempBeginField, tempEndField, tempHashFields);
    }
}
