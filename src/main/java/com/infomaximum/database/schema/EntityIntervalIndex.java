package com.infomaximum.database.schema;

import com.infomaximum.database.anotation.IntervalIndex;
import com.infomaximum.database.exception.runtime.IllegalTypeException;

import java.time.Instant;
import java.util.List;

public class EntityIntervalIndex extends BaseIndex {

    public EntityIntervalIndex(IntervalIndex index, StructEntity parent) {
        super(buildIndexedFields(index, parent), parent);
    }

    public List<EntityField> getHashedFields() {
        return sortedFields.subList(0, sortedFields.size() - 1);
    }

    public EntityField getIndexedField() {
        return sortedFields.get(sortedFields.size() - 1);
    }

    @Override
    protected String getIndexName() {
        return "interval_index";
    }

    private static List<EntityField> buildIndexedFields(IntervalIndex index, StructEntity parent) {
        EntityField indexedField = parent.getField(index.indexedField());
        if (!Number.class.isAssignableFrom(indexedField.getType()) && indexedField.getType() != Instant.class) {
            throw new IllegalTypeException(indexedField.getType(), Number.class);
        }

        List<EntityField> fields = buildIndexedFields(index.hashedFields(), parent);
        fields.add(indexedField);
        return fields;
    }
}
