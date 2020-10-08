package com.infomaximum.database.engine;

import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBDataReader;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.dbstruct.DBBaseIntervalIndex;
import com.infomaximum.database.schema.dbstruct.DBTable;
import com.infomaximum.database.utils.key.IntervalIndexKey;

public class IntervalIterator extends BaseIntervalRecordIterator<IntervalFilter> {

    public IntervalIterator(DBTable table, IntervalFilter filter, DBDataReader dataReader) {
        super(table, filter, filter.getSortDirection(), dataReader);
    }

    @Override
    DBBaseIntervalIndex getIndex(IntervalFilter filter, DBTable table) {
        return table.getIndex(filter);
    }

    @Override
    KeyValue seek(DBIterator indexIterator, KeyPattern pattern) throws DatabaseException {
        return indexIterator.seek(pattern);
    }

    @Override
    int matchKey(long id, byte[] key) {
        long indexedBeginValue = IntervalIndexKey.unpackIndexedValue(key);
        if (indexedBeginValue < filterBeginValue || indexedBeginValue > filterEndValue) {
            return KeyPattern.MATCH_RESULT_UNSUCCESS;
        }
        return KeyPattern.MATCH_RESULT_SUCCESS;
    }
}
