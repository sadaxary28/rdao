package com.infomaximum.database.core.structentity;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.anotation.Index;
import com.infomaximum.database.exeption.runtime.StructEntityDatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by kris on 24.05.17.
 */
public class StructEntityIndex {

    private final static Logger log = LoggerFactory.getLogger(StructEntityIndex.class);

    public final String columnFamily;
    public final List<Field> sortedFields;

    public StructEntityIndex(StructEntity structEntity, Index index) throws StructEntityDatabaseException {
        List<Field> modifiableIndexFields = new ArrayList<>();
        for (String fieldName: index.fields()) {
            Field field = structEntity.getFieldByName(fieldName);
            if (field==null) throw new StructEntityDatabaseException("Not found index: " + fieldName);

            modifiableIndexFields.add(field);
        }

        //Сортируем, что бы хеш не ломался из-за перестановки местами полей
        Collections.sort(modifiableIndexFields, (o1, o2) -> o1.name().toLowerCase().compareTo(o2.name().toLowerCase()));

        //Вычисляем columnFamily индекса
        this.columnFamily = structEntity.annotationEntity.name() + ".index." + buildColumnFamilyIndexToSortFields(modifiableIndexFields);

        this.sortedFields = Collections.unmodifiableList(modifiableIndexFields);
    }

    private static String buildColumnFamilyIndexToSortFields(Collection<Field> modifiableIndexFields){
        StringJoiner fieldJoiner = new StringJoiner(".");
        for (Field field: modifiableIndexFields) {
            fieldJoiner.add(field.name() + ":" + field.type().getName());
        }
        return fieldJoiner.toString();
    }


}
