package com.infomaximum.database.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.exception.runtime.DatabaseRuntimeException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.schema.dbstruct.DBTable;

public class AllIterator extends BaseRecordIterator {

    private final DBIterator iterator;
    private final DBTable table;
    private final NextState state;

    public AllIterator(DBTable table, DBDataReader dataReader) throws DatabaseRuntimeException, DatabaseException {
        this.iterator = dataReader.createIterator(table.getDataColumnFamily());
        this.table = table;
        state = initializeState();
    }

    @Override
    public boolean hasNext() throws DatabaseRuntimeException {
        return !state.isEmpty();
    }

    @Override
    public Record next() throws DatabaseRuntimeException {
        try {
            return nextRecord(table, state, iterator);
        } catch (DatabaseException e) {
            throw new DatabaseRuntimeException(e);
        }
    }

    @Override
    public void close() throws DatabaseRuntimeException {
        try {
            iterator.close();
        } catch (DatabaseException e) {
            throw new DatabaseRuntimeException(e);
        }
    }

    private NextState initializeState() throws DatabaseRuntimeException {
        try {
            return seek(null, iterator);
        } catch (DatabaseException e) {
            throw new DatabaseRuntimeException(e);
        }
    }
}
