package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.utils.key.IntervalIndexKey;

import java.util.*;

public class IntervalIndexIterator<E extends DomainObject> extends BaseIntervalIndexIterator<E, IntervalFilter> {

    public IntervalIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, IntervalFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields, filter.getSortDirection(), filter);
    }

    @Override
    BaseIntervalIndex getIndex(IntervalFilter filter, StructEntity entity) {
        return entity.getIntervalIndex(filter.getHashedValues().keySet(), filter.getIndexedFieldName());
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
