package com.infomaximum.database.core.structentity;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.exeption.runtime.FieldNotFoundDatabaseException;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;

import java.util.*;

/**
 * Created by kris on 24.05.17.
 */
public class StructEntityIndex {

    public final String columnFamily;
    public final List<Field> sortedFields;

    public StructEntityIndex(StructEntity structEntity, Index index) throws StructEntityDatabaseException {
        List<Field> modifiableIndexFields = new ArrayList<>(index.fields().length);
        for (String fieldName: index.fields()) {
            modifiableIndexFields.add(structEntity.getFieldByName(fieldName));
        }

        //Сортируем, что бы хеш не ломался из-за перестановки местами полей
        Collections.sort(modifiableIndexFields, (o1, o2) -> o1.name().toLowerCase().compareTo(o2.name().toLowerCase()));

        this.columnFamily = buildColumnFamilyName(structEntity.annotationEntity.name(), modifiableIndexFields);
        this.sortedFields = Collections.unmodifiableList(modifiableIndexFields);
    }

    private static String buildColumnFamilyName(String parentColumnFamily, List<Field> modifiableIndexFields){
        StringBuilder stringBuilder = new StringBuilder(parentColumnFamily).append(".index");
        modifiableIndexFields.forEach(field -> stringBuilder.append('.').append(field.name()).append(':').append(field.type().getName()));
        return stringBuilder.toString();
    }
}
