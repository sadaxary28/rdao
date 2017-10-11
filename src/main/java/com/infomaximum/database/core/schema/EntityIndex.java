package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;

import java.util.*;

/**
 * Created by kris on 24.05.17.
 */
public class EntityIndex {

    public final String columnFamily;
    public final List<EntityField> sortedFields;

    EntityIndex(Index index, StructEntity parent) {
        List<EntityField> modifiableIndexFields = new ArrayList<>(index.fields().length);
        for (String fieldName: index.fields()) {
            modifiableIndexFields.add(parent.getField(fieldName));
        }

        //Сортируем, что бы хеш не ломался из-за перестановки местами полей
        modifiableIndexFields.sort(Comparator.comparing(o -> o.getName().toLowerCase()));

        this.sortedFields = Collections.unmodifiableList(modifiableIndexFields);
        this.columnFamily = buildColumnFamilyName(parent.getColumnFamily(), this.sortedFields);
    }

    EntityIndex(EntityField field, StructEntity parent) {
        this.sortedFields = Collections.singletonList(field);
        this.columnFamily = buildColumnFamilyName(parent.getColumnFamily(), this.sortedFields);
    }

    private static String buildColumnFamilyName(String parentColumnFamily, List<EntityField> modifiableIndexFields){
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(StructEntity.NAMESPACE_SEPARATOR).append("index");
        modifiableIndexFields.forEach(field -> stringBuilder.append('.').append(field.getName()).append(':').append(field.getType().getName()));
        return stringBuilder.toString();
    }
}
