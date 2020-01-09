package com.infomaximum.database.domainobject.iterator;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.PrefixFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.KeyPattern;
import com.infomaximum.database.provider.KeyValue;
import com.infomaximum.database.schema.Field;
import com.infomaximum.database.schema.PrefixIndex;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;
import com.infomaximum.database.utils.key.PrefixIndexKey;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class PrefixIndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final PrefixIndex index;

    private List<String> searchingWords;
    private ByteBuffer loadingIds = null;
    private RangeSet<Long> prevLoadedIds = TreeRangeSet.create();
    private String[] values;

    private List<String> tempList;

    public PrefixIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<Integer> loadingFields, PrefixFilter filter) throws DatabaseException {
        super(dataEnumerable, clazz, loadingFields);
        this.index = entity.getPrefixIndex(filter.getFieldNames());
        this.searchingWords = PrefixIndexUtils.splitSearchingTextIntoWords(filter.getFieldValue());
        if (this.searchingWords.isEmpty()) {
            return;
        }

        KeyPattern indexKeyPattern = PrefixIndexKey.buildKeyPatternForFind(searchingWords.get(searchingWords.size() - 1), index);
        List<Field> additionLoadingFields;
        if (this.searchingWords.size() > 1) {
            additionLoadingFields = index.sortedFields;
        } else {
            additionLoadingFields = Collections.emptyList();
            this.searchingWords = Collections.emptyList();
        }

        this.dataKeyPattern = buildDataKeyPattern(additionLoadingFields, loadingFields, entity);
        if (this.dataKeyPattern != null) {
            this.dataIterator = dataEnumerable.createIterator(entity.getColumnFamily());
            this.values = new String[index.sortedFields.size()];
            this.tempList = new ArrayList<>();
        }

        this.indexIterator = dataEnumerable.createIterator(index.columnFamily);
        KeyValue keyValue = indexIterator.seek(indexKeyPattern);
        this.loadingIds = keyValue != null ? TypeConvert.wrapBuffer(keyValue.getValue()) : null;

        nextImpl();
    }

    @Override
    void nextImpl() throws DatabaseException {
        while (loadingIds != null) {
            if (!loadingIds.hasRemaining()) {
                KeyValue keyValue = indexIterator.next();
                loadingIds = keyValue != null ? TypeConvert.wrapBuffer(keyValue.getValue()) : null;
                continue;
            }

            final long id = loadingIds.getLong();
            if (prevLoadedIds.contains(id)) {
                continue;
            }

            nextElement = findObject(id);
            if (nextElement != null) {
                prevLoadedIds.add(Range.closedOpen(id, id + 1));
                return;
            }
        }

        nextElement = null;
        close();
    }

    @Override
    boolean checkFilter(E obj) throws DatabaseException {
        for (int i = 0; i < index.sortedFields.size(); ++i) {
            values[i] = obj.get(index.sortedFields.get(i).getNumber());
        }
        return PrefixIndexUtils.contains(searchingWords, values, tempList);
    }
}

