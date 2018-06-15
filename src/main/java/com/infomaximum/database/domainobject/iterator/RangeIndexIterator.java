package com.infomaximum.database.domainobject.iterator;

import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.domainobject.filter.SortDirection;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.*;
import com.infomaximum.database.utils.RangeIndexUtils;
import com.infomaximum.database.utils.key.RangeIndexKey;

import java.util.*;

public class RangeIndexIterator<E extends DomainObject> extends BaseIntervalIndexIterator<E, RangeFilter> {

    private Set<Long> processedIds/* = null*/; // не нужно инициализировать, т.к. matchKey вызывается из конструктора базового класса

    public RangeIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, RangeFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields, SortDirection.ASC, filter);
    }

    @Override
    BaseIntervalIndex getIndex(RangeFilter filter, StructEntity entity) {
        RangeFilter.IndexedField indexedField = filter.getIndexedField();
        return entity.getRangeIndex(filter.getHashedValues().keySet(), indexedField.beginField, indexedField.endField);
    }

    @Override
    KeyValue seek(DBIterator indexIterator, KeyPattern pattern) throws DatabaseException {
        return RangeIndexUtils.seek(indexIterator, pattern, filterBeginValue);
    }

    @Override
    int matchKey(long id, byte[] key) {
        long indexedValue = RangeIndexKey.unpackIndexedValue(key);
        if (indexedValue >= filterEndValue) {
            return KeyPattern.MATCH_RESULT_UNSUCCESS;
        }

        if (processedIds != null && processedIds.contains(id)) {
            if (RangeIndexKey.unpackEndOfRange(key)) {
                processedIds.remove(id);
            }
            return KeyPattern.MATCH_RESULT_CONTINUE;
        }

        if (!RangeIndexKey.unpackEndOfRange(key)) {
            if (processedIds == null) {
                processedIds = new HashSet<>();
            }
            processedIds.add(id);
        }
        return KeyPattern.MATCH_RESULT_SUCCESS;
    }
}
