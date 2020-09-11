package com.infomaximum.database.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.RecordIterator;
import com.infomaximum.database.exception.runtime.DatabaseRuntimeException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.schema.dbstruct.DBField;
import com.infomaximum.database.schema.dbstruct.DBTable;

public class AllIterator implements RecordIterator {

    public AllIterator(DBTable table, DBField[] selectingFields, DBDataReader dataReader) {
        // TODO realize
    }
//    @Override
//    public void reuseReturningRecord(boolean value) {
//        // TODO realize
//    }

    @Override
    public boolean hasNext() throws DatabaseRuntimeException {
        // TODO realize
        return false;
    }

    @Override
    public Record next() throws DatabaseRuntimeException {
        // TODO realize
        return null;
    }

    @Override
    public void close() throws DatabaseRuntimeException {
        // TODO realize
    }
}
