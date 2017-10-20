package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class BaseIndex {

    public final String columnFamily;
    public final List<EntityField> sortedFields;

    BaseIndex(Index index, StructEntity parent) {
        List<EntityField> modifiableIndexFields = new ArrayList<>(index.fields().length);
        for (String fieldName: index.fields()) {
            modifiableIndexFields.add(parent.getField(fieldName));
        }

        //Сортируем, что бы хеш не ломался из-за перестановки местами полей
        modifiableIndexFields.sort(Comparator.comparing(o -> o.getName().toLowerCase()));

        this.sortedFields = Collections.unmodifiableList(modifiableIndexFields);
        this.columnFamily = buildColumnFamilyName(parent.getColumnFamily(), this.sortedFields);
    }

    BaseIndex(EntityField field, StructEntity parent) {
        this.sortedFields = Collections.singletonList(field);
        this.columnFamily = buildColumnFamilyName(parent.getColumnFamily(), this.sortedFields);
    }

    abstract String getTypeMarker();

    private String buildColumnFamilyName(String parentColumnFamily, List<EntityField> modifiableIndexFields){
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(StructEntity.NAMESPACE_SEPARATOR).append(getTypeMarker());
        modifiableIndexFields.forEach(field -> stringBuilder.append('.').append(field.getName()).append(':').append(field.getType().getName()));
        return stringBuilder.toString();
    }
}