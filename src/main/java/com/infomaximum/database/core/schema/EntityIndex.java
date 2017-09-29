package com.infomaximum.database.core.schema;

import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;

import java.util.*;

/**
 * Created by kris on 24.05.17.
 */
public class EntityIndex {

    public final String columnFamily;
    public final List<EntityField> sortedFields;

    protected EntityIndex(Index index, StructEntity parent) throws StructEntityDatabaseException {
        List<EntityField> modifiableIndexFields = new ArrayList<>(index.fields().length);
        for (String fieldName: index.fields()) {
            modifiableIndexFields.add(parent.getField(fieldName));
        }

        //Сортируем, что бы хеш не ломался из-за перестановки местами полей
        Collections.sort(modifiableIndexFields, (o1, o2) -> o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase()));

        this.columnFamily = buildColumnFamilyName(parent.getName(), modifiableIndexFields);
        this.sortedFields = Collections.unmodifiableList(modifiableIndexFields);
    }

    private static String buildColumnFamilyName(String parentColumnFamily, List<EntityField> modifiableIndexFields){
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(".index");
        modifiableIndexFields.forEach(field -> stringBuilder.append('.').append(field.getName()).append(':').append(field.getType().getName()));
        return stringBuilder.toString();
    }
}
