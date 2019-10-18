package com.infomaximum.database.schema.impl;

import java.util.Arrays;

public abstract class DBIndex extends DBObject {

    private final int[] fieldIds;

    DBIndex(int id, int[] fieldIds) {
        super(id);
        this.fieldIds = fieldIds;
    }

    public int[] getFieldIds() {
        return fieldIds;
    }

    public boolean fieldContains(int fieldId) {
        return contains(fieldId, getFieldIds());
    }

    public boolean fieldsEquals(DBIndex index) {
        return Arrays.equals(index.fieldIds, fieldIds);
    }

    private static boolean contains(int value, int[] destination) {
        for (int i = 0; i < destination.length; ++i) {
            if (destination[i] == value) {
                return true;
            }
        }
        return false;
    }

    static void checkSorting(int[] fieldIds) {
        if (fieldIds.length < 2) {
            return;
        }

        for (int i = 1; i < fieldIds.length; ++i) {
            if (fieldIds[i - 1] >= fieldIds[i]) {
                throw new IllegalArgumentException("wrong sorting");
            }
        }
    }
}
