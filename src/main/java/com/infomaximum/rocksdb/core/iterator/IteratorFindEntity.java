package com.infomaximum.rocksdb.core.iterator;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.datasource.entitysource.EntitySource;
import com.infomaximum.rocksdb.exception.NotFoundIndexException;
import com.infomaximum.rocksdb.utils.EqualsUtils;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

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

    public IteratorFindEntity(DataSource dataSource, Class<E> clazz, Map<String, Object> filters) throws ReflectiveOperationException, RocksDBException {
        this.dataSource = dataSource;
        this.clazz = clazz;

        this.structEntity = HashStructEntities.getStructEntity(clazz);

        StructEntityIndex structEntityIndex = structEntity.getStructEntityIndex(filters.keySet());
        if (structEntityIndex==null) throw new NotFoundIndexException(clazz, filters.keySet());

        this.getterFilters = new HashMap<Method, Object>();
        for (Map.Entry<String, Object> filter: filters.entrySet()) {
            String fieldName = filter.getKey();
            Object value = filter.getValue();

            Field field = structEntity.getFieldByName(fieldName);
            if (field==null) throw new RuntimeException("Not found field " + fieldName + ", to " + clazz.getName());

//            getterFilters.put(structEntity.getGetterMethodByField(field), value);
        }

        this.nameIndex = structEntityIndex.name;

        //Сортируем поля и вычисляем хеш
        //TODO мигрировать
        this.hash = 0;
//        List<Object> sortFilterValues = new ArrayList();
//        for (Field field: structEntityIndex.indexFieldsSort) {
//            sortFilterValues.add(filters.get(field.getName()));
//        }
//        this.hash = IndexUtils.calcHashValues(sortFilterValues);

        nextElement = loadNextElement(true);
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(boolean isFirst) throws RocksDBException, ReflectiveOperationException {
        Long prevFindId = (isFirst)?null:nextElement.getId();

        E domainObject = null;
        while (true) {
            EntitySource entitySource = dataSource.findNextEntitySource(structEntity.annotationEntity.name(), prevFindId, nameIndex, hash, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
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
