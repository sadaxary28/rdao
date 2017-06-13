package com.infomaximum.rocksdb.core.iterator;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.index.IndexUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntityIndex;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.utils.EqualsUtils;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorFindEntity<E extends DomainObject> implements Iterator<E>, Iterable<E> {

    private final DataSource dataSource;
    private final Class<E> clazz;
    private final StructEntity structEntity;

    private final String nameIndex;
    private final int hash;

    private final Map<Method, Object> getterFilters;

    private E nextElement;

//    public <E extends DomainObject> IteratorFindEntity(DataSource dataSource, Class<E> clazz, Map<String, Object> filters) throws NoSuchMethodException, InstantiationException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, RocksDBException {
    public IteratorFindEntity(DataSource dataSource, Class<E> clazz, Map<String, Object> filters) throws NoSuchMethodException, InstantiationException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, RocksDBException {
        this.dataSource = dataSource;
        this.clazz = clazz;

        this.structEntity = HashStructEntities.getStructEntity(clazz);

        this.getterFilters = new HashMap<Method, Object>();
        List<Field> indexFields = new ArrayList<Field>();
        for (Map.Entry<String, Object> filter: filters.entrySet()) {
            String fieldName = filter.getKey();
            Object value = filter.getValue();

            Field field = structEntity.getFieldByName(fieldName);
            if (field==null) throw new RuntimeException("Not found field " + fieldName + ", to " + clazz.getName());
            if (!EqualsUtils.equalsType(field.getType(), value.getClass())) throw new RuntimeException("Not equals type field " + field.getType() + " and type value " + value.getClass());

            indexFields.add(field);
            getterFilters.put(structEntity.getGetterMethodByField(field), value);
        }

        this.nameIndex = StructEntityIndex.buildNameIndex(indexFields);
        this.hash = IndexUtils.calcHashValues(filters.values());

        nextElement = loadNextElement(true);
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(boolean isFirst) throws RocksDBException, NoSuchMethodException, NoSuchFieldException, InstantiationException, IllegalAccessException, InvocationTargetException {
        Long prevFindId = (isFirst)?null:nextElement.getId();

        E domainObject = null;
        while (true) {
            EntitySource entitySource = dataSource.findNextEntitySource(structEntity.annotationEntity.columnFamily(), prevFindId, nameIndex, hash, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
            if (entitySource==null) break;

            domainObject = DomainObjectUtils.createDomainObject(dataSource, clazz, entitySource);

            //Необходима дополнительная проверка, так как нельзя исключать сломанный индекс или коллизии хеша
            boolean isFullCoincidence = true;
            for (Map.Entry<Method, Object> entry: getterFilters.entrySet()) {
                Method getterToField = entry.getKey();
                Object value = entry.getValue();

                Object iValue = getterToField.invoke(domainObject);
                if (!EqualsUtils.equals(iValue, value)) {
                    //Промахнулись с индексом
                    isFullCoincidence=false;
                    break;
                }
            }
            if (isFullCoincidence) {
                //Все хорошо, совпадение полное - выходим
                break;
            } else {
                //Промахнулись с индексом - уходим на повторный круг
                prevFindId = domainObject.getId();
                domainObject = null;
            }
        }

        if (domainObject==null) {
            nextElement = null;
        } else {
            nextElement = domainObject;
        }
        return nextElement;
    }

    @Override
    public boolean hasNext() {
        return (nextElement!=null);
    }

    @Override
    public E next() {
        if (nextElement==null) throw new NoSuchElementException();

        E element = nextElement;
        try {
            nextElement = loadNextElement(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return element;
    }

    @Override
    public Iterator<E> iterator() {
        return this;
    }

}
