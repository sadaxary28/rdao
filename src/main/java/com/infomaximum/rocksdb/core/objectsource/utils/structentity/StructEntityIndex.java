package com.infomaximum.rocksdb.core.objectsource.utils.structentity;

import com.infomaximum.rocksdb.core.anotation.Index;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by kris on 24.05.17.
 */
public class StructEntityIndex {

    private final static Logger log = LoggerFactory.getLogger(StructEntityIndex.class);

    public final String name;
    public final List<Field> indexFieldsSort;

    public StructEntityIndex(StructEntity structEntity, Index index) {
        List<Field> modifiableIndexFields = new ArrayList<>();
        for (String fieldName: index.fieldNames()) {
            Field field;
            try {
                field = structEntity.clazz.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                throw new RuntimeException("Error build domain index from " + structEntity.clazz.getName() + " not found field: " + fieldName, e);
            }
            field.setAccessible(true);
            modifiableIndexFields.add(field);
        }

        //Сортируем, что бы хеш не ломался при перестановки местами полей
        Collections.sort(modifiableIndexFields, (o1, o2) -> o1.getName().toLowerCase().compareTo(o2.getName().toLowerCase()));

        //Вычисляем имя индекса
        List<String> sortIndexFields = modifiableIndexFields.stream().map(Field::getName).collect(Collectors.toList());
        this.name = buildNameIndexToSortFields(sortIndexFields);

        this.indexFieldsSort = Collections.unmodifiableList(modifiableIndexFields);
    }

    public static String buildNameIndex(Collection<String> nameIndexFields){
        List<String> sortIndexFields = new ArrayList<>(nameIndexFields);
        Collections.sort(sortIndexFields, (o1, o2) -> o1.toLowerCase().compareTo(o2.toLowerCase()));
        return buildNameIndexToSortFields(sortIndexFields);
    }

    private static String buildNameIndexToSortFields(Collection<String> sortNameIndexFields){
        StringJoiner fieldJoiner = new StringJoiner(".");
        for (String nameField: sortNameIndexFields) {
            fieldJoiner.add(nameField);
        }
        return buildNameIndex(fieldJoiner.toString());
    }


    public static String buildNameIndex(Field field){
        return buildNameIndex(field.getName().toString());
    }

    private static String buildNameIndex(String value) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(value.getBytes(TypeConvertRocksdb.ROCKSDB_CHARSET));
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1, digest);
            StringBuilder h = new StringBuilder(bigInt.toString(16));
            while(h.length() < 32){
                h.insert(0, "0");
            }
            return h.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("No MD5 algorithm", e);
        }
    }
}
