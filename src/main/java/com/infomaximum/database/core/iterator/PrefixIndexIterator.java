package com.infomaximum.database.core.iterator;

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
            this.dataIteratorId = dataEnumerable.createIterator(structEntity.getColumnFamily(), null);
            this.values = new String[entityIndex.sortedFields.size()];
            this.tempList = new ArrayList<>();
        }

        this.indexIteratorId = dataEnumerable.createIterator(entityIndex.columnFamily, indexKeyPattern);

        nextImpl();
    }

    @Override
    void nextImpl() throws DataSourceDatabaseException {
        while (true) {
            if (loadingIds == null || !loadingIds.hasRemaining()) {
                KeyValue keyValue = dataEnumerable.next(indexIteratorId);
                if (keyValue == null) {
                    break;
                }

                loadingIds = TypeConvert.wrapBuffer(keyValue.getValue());
                continue;
            }

            nextElement = findObject(loadingIds.getLong());
            if (nextElement != null) {
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

