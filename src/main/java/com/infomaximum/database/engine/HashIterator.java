package com.infomaximum.database.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.IllegalTypeException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBHashIndex;
import com.infomaximum.database.schema.dbstruct.DBTable;
import com.infomaximum.database.utils.HashIndexUtils;
import com.infomaximum.database.utils.key.HashIndexKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashIterator extends BaseIndexRecordIterator {

    private final Map<DBField, Object> filterFieldsValue = new HashMap<>();
    private KeyValue indexKeyValue;

    public HashIterator(DBTable table, HashFilter filter, DBDataReader dataReader) {
        super(table, dataReader);

        Map<Integer, Object> filters = filter.getValues();
        final DBHashIndex index = table.getIndex(filter);

        List<DBField> filterFields = new ArrayList<>();

        long[] values = new long[index.getFieldIds().length];
        for (int i = 0; i < index.getFieldIds().length; ++i) {
            DBField field = table.getField(index.getFieldIds()[i]);
            Object value = filters.get(field.getId());
            checkValueType(value, field);

            values[i] = HashIndexUtils.buildHash(field.getType(), value, null);
            if (HashIndexUtils.toLongCastable(field.getType())) {
                continue;
            }
            filterFields.add(field);
            filterFieldsValue.put(field, value);
        }
        this.indexKeyValue = indexIterator.seek(HashIndexKey.buildKeyPattern(index, values));

        nextImpl();
    }

    @Override
    protected void nextImpl() throws DatabaseException {
        while (indexKeyValue != null) {
            nextRecord = findRecord(HashIndexKey.unpackId(indexKeyValue.getKey()));
            indexKeyValue = indexIterator.next();
            if (nextRecord != null) {
                return;
            }
        }

        nextRecord = null;
        close();
    }

    @Override
    boolean checkFilter(Record record) throws DatabaseException {
        return filterFieldsValue.entrySet()
                .stream()
                .allMatch(fieldEntry -> HashIndexUtils.equals(fieldEntry.getKey().getType(), fieldEntry.getValue(), record.getValues()[fieldEntry.getKey().getId()]));
    }

    private void checkValueType(Object value, DBField field) {
        if (value != null && field.getType() != value.getClass()) {
            throw new IllegalTypeException(field.getType(), value.getClass());
        }
    }
}
