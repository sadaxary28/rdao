package com.infomaximum.database.engine;

import com.infomaximum.database.Record;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.schema.dbstruct.DBTable;

public class AllIterator extends BaseRecordIterator {

    private final DBIterator iterator;
    private final DBTable table;
    private final NextState state;

    public AllIterator(DBTable table, DBDataReader dataReader) throws DatabaseException {
        this.iterator = dataReader.createIterator(table.getDataColumnFamily());
        this.table = table;
        state = initializeState();
    }

    @Override
    public boolean hasNext() throws DatabaseException {
        return !state.isEmpty();
    }

    @Override
    public Record next() throws DatabaseException {
        try {
            return nextRecord(table, state, iterator);
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() throws DatabaseException {
        try {
            iterator.close();
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }

    private NextState initializeState() throws DatabaseException {
        try {
            return seek(null, iterator);
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }
}
