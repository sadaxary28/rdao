package com.infomaximum.database.core.iterator;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.EntityPrefixIndex;
import com.infomaximum.database.core.schema.Schema;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
import com.infomaximum.database.domainobject.key.PrefixIndexKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.PrefixIndexUtils;
import com.infomaximum.database.utils.TypeConvert;

import java.nio.ByteBuffer;
import java.util.*;

public class PrefixIndexIterator<E extends DomainObject> extends BaseIndexIterator<E> {

    private final EntityPrefixIndex entityIndex;

    private List<String> searchingWords;
    private ByteBuffer loadingIds = null;
    private RangeSet<Long> prevLoadedIds = TreeRangeSet.create();
    private String[] values;

    private List<String> tempList;

    public PrefixIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, PrefixIndexFilter filter) throws DataSourceDatabaseException {
        super(dataEnumerable, clazz, loadingFields);
        StructEntity structEntity = Schema.getEntity(clazz);
        this.entityIndex = structEntity.getPrefixIndex(filter.getFieldNames());
        this.searchingWords = PrefixIndexUtils.splitSearchingTextIntoWords(filter.getFieldValue());
        if (this.searchingWords.isEmpty()) {
            return;
        }

        KeyPattern indexKeyPattern = PrefixIndexKey.buildKeyPatternForFind(searchingWords.get(searchingWords.size() - 1));
        List<EntityField> additionLoadingFields;
        if (this.searchingWords.size() > 1) {
            additionLoadingFields = entityIndex.sortedFields;
        } else {
            additionLoadingFields = Collections.emptyList();
            this.searchingWords = Collections.emptyList();
        }

        this.dataKeyPattern = buildDataKeyPattern(additionLoadingFields, loadingFields);
        if (this.dataKeyPattern != null) {
            this.dataIteratorId = dataEnumerable.createIterator(structEntity.getColumnFamily());
            this.values = new String[entityIndex.sortedFields.size()];
            this.tempList = new ArrayList<>();
        }

        this.indexIteratorId = dataEnumerable.createIterator(entityIndex.columnFamily);
        KeyValue keyValue = dataEnumerable.seek(indexIteratorId, indexKeyPattern);
        this.loadingIds = keyValue != null ? TypeConvert.wrapBuffer(keyValue.getValue()) : null;

        nextImpl();
    }

    @Override
    void nextImpl() throws DataSourceDatabaseException {
        while (loadingIds != null) {
            if (!loadingIds.hasRemaining()) {
                KeyValue keyValue = dataEnumerable.next(indexIteratorId);
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
    boolean checkFilter(E obj) throws DataSourceDatabaseException {
        for (int i = 0; i < entityIndex.sortedFields.size(); ++i) {
            values[i] = obj.get(String.class, entityIndex.sortedFields.get(i).getName());
        }
        return PrefixIndexUtils.contains(searchingWords, values, tempList);
    }
}

