package com.infomaximum.database.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.UnexpectedEndObjectException;
import com.infomaximum.database.exception.runtime.DatabaseRuntimeException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBTable;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.FieldKey;

import java.util.List;
import java.util.NoSuchElementException;

public abstract class BaseRecordIterator implements RecordIterator {

    public static class NextState {

        private long nextId = -1;

        private NextState(long recordId) {
            this.nextId = recordId;
        }

        public boolean isEmpty() {
            return nextId == -1;
        }
    }

    protected Record nextRecord(DBTable table, NextState state, DBIterator dbIterator) throws DatabaseRuntimeException, DatabaseException {
        if (state.isEmpty()) {
            throw new NoSuchElementException();
        }
        return readObject(table, state, dbIterator);
    }

    protected NextState seek(KeyPattern pattern, DBIterator iterator) throws DatabaseRuntimeException, DatabaseException {
        KeyValue keyValue = iterator.seek(pattern);
        if (keyValue == null) {
            return new NextState(-1);
        }

        if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
            return new NextState(-1);
        }

        long objId = FieldKey.unpackId(keyValue.getKey());
        return new NextState(objId);
    }

    private <T extends DomainObject> Record readObject(DBTable table, NextState state, DBIterator iterator) throws DatabaseException {
        long recordId = state.nextId;
        List<DBField> fields = table.getSortedFields();
        Object[] values = new Object[fields.size()];
        KeyValue keyValue;
        while ((keyValue = iterator.next()) != null) {
            long id = FieldKey.unpackId(keyValue.getKey());
            if (id != recordId) {
                if (!FieldKey.unpackBeginningObject(keyValue.getKey())) {
                    throw new UnexpectedEndObjectException(recordId, id, FieldKey.unpackFieldName(keyValue.getKey()));
                }
                state.nextId = id;
                return new Record(recordId, values);
            }
            DBField field = table.getField(FieldKey.unpackFieldName(keyValue.getKey()));
            values[field.getId()] = TypeConvert.unpack(field.getType(), keyValue.getValue(), null);
        }
        state.nextId = -1;
        return new Record(recordId, values);
    }
}
