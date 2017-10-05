package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.schema.EntityPrefixIndex;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.datasource.KeyValue;
import com.infomaximum.database.domainobject.DataEnumerable;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.filter.PrefixIndexFilter;
import com.infomaximum.database.domainobject.key.IndexKey;
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

    public PrefixIndexIterator(DataEnumerable dataEnumerable, Class<E> clazz, Set<String> loadingFields, PrefixIndexFilter filter) throws DataSourceDatabaseException {
        super(dataEnumerable, clazz);
        StructEntity structEntity = StructEntity.getInstance(clazz);
        this.entityIndex = structEntity.getPrefixIndexes()
                .stream()
                .filter(entityPrefixIndex -> entityPrefixIndex.field.getName().equals(filter.getFieldName()))
                .findFirst()
                .get();

        this.searchingWords = PrefixIndexUtils.splitSearchingTextIntoWords(filter.getFieldValue());
        if (this.searchingWords.isEmpty()) {
            return;
        }

        KeyPattern indexKeyPattern = PrefixIndexKey.buildKeyPatternForFind(searchingWords.get(searchingWords.size() - 1));
        List<EntityField> additionLoadingFields;
        if (this.searchingWords.size() > 1) {
            additionLoadingFields = Collections.singletonList(entityIndex.field);

        } else {
            additionLoadingFields = Collections.emptyList();
            this.searchingWords = Collections.emptyList();
        }

        this.dataKeyPattern = buildDataKeyPattern(additionLoadingFields, loadingFields);
        if (this.dataKeyPattern != null) {
            this.dataIteratorId = dataEnumerable.createIterator(structEntity.getName(), null);
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
        return PrefixIndexUtils.contains(searchingWords, obj.get(String.class, entityIndex.field.getName()));
    }
}

