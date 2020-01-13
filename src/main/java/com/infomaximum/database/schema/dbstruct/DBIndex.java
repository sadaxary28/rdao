package com.infomaximum.database.schema.dbstruct;

import com.infomaximum.database.utils.key.KeyUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class DBIndex extends DBObject {

    private static final int FIELDS_HASH_BYTE_SIZE = 4;
    private static final int INDEX_NAME_BYTE_SIZE = 3;
    public static final int ATTENDANT_BYTE_SIZE = INDEX_NAME_BYTE_SIZE + FIELDS_HASH_BYTE_SIZE;

    public final byte[] attendant;
    private final List<DBField> fields;
    private final int[] fieldIds;

    DBIndex(int id, List<DBField> sortedFields) {
        super(id);
        this.fields = sortedFields;
        fieldIds = sortedFields.stream().mapToInt(DBObject::getId).toArray();
        this.attendant = KeyUtils.buildIndexAttendant(getIndexNameBytes(), sortedFields);
    }

    public List<DBField> getFields() {
        return Collections.unmodifiableList(fields);
    }

    public byte[] getAttendant() {
        return attendant;
    }

    public boolean fieldContains(int fieldId) {
        return contains(fieldId, fieldIds);
    }

    public boolean fieldsEquals(DBIndex index) {
        return Arrays.equals(index.fieldIds, fieldIds);
    }

    private static boolean contains(int value, int[] destination) {
        for (int item : destination) {
            if (item == value) {
                return true;
            }
        }
        return false;
    }

    static void checkSorting(List<DBField> fields) {
        if (fields.size() < 2) {
            return;
        }

        for (int i = 1; i < fields.size(); ++i) {
            if (fields.get(i - 1).getId() >= fields.get(i).getId()) {
                throw new IllegalArgumentException("wrong sorting");
            }
        }
    }

    protected abstract byte[] getIndexNameBytes();
}
