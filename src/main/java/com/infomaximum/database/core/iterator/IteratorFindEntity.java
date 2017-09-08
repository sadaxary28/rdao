package com.infomaximum.database.core.iterator;

import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.index.IndexUtils;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.entitysource.EntitySource;
import com.infomaximum.database.domainobject.DomainObject;
import com.infomaximum.database.domainobject.DomainObjectUtils;
import com.infomaximum.database.exeption.index.NotFoundIndexDatabaseException;
import com.infomaximum.database.utils.EqualsUtils;

import java.util.*;

/**
 * Created by kris on 30.04.17.
 */
public class IteratorFindEntity<E extends DomainObject> implements Iterator<E>, Iterable<E> {

    private final DataSource dataSource;
    private final Class<E> clazz;
    private final Map<String, Object> filters;

    private final StructEntity structEntity;
    private final StructEntityIndex structEntityIndex;

    private final int findHash;

    private E nextElement;

    public IteratorFindEntity(DataSource dataSource, Class<E> clazz, Map<String, Object> filters) {
        this.dataSource = dataSource;
        this.clazz = clazz;
        this.filters=filters;

        this.structEntity = HashStructEntities.getStructEntity(clazz);

        structEntityIndex = structEntity.getStructEntityIndex(filters.keySet());
        if (structEntityIndex==null) throw new NotFoundIndexDatabaseException(clazz, filters.keySet());

        //Проверяем совпадение типов
        for (Field field: structEntityIndex.indexFieldsSort) {
            Object filterValue = filters.get(field.name());
            if (filterValue!=null && !EqualsUtils.equalsType(field.type(), filterValue.getClass())) throw new RuntimeException("Not equals type field " + field.type() + " and type value " + filterValue.getClass());
        }

        //Сортируем поля и вычисляем хеш
        List<Object> sortFilterValues = new ArrayList();
        for (Field field: structEntityIndex.indexFieldsSort) {
            sortFilterValues.add(filters.get(field.name()));
        }
        this.findHash = IndexUtils.calcHashValues(sortFilterValues);

        nextElement = loadNextElement(true);
    }

    /** Загружаем следующий элемент */
    private synchronized E loadNextElement(boolean isFirst) {
        Long prevFindId = (isFirst)?null:nextElement.getId();

        E domainObject = null;
        while (true) {
            EntitySource entitySource = dataSource.findNextEntitySource(structEntity.annotationEntity.name(), prevFindId, structEntityIndex.name, findHash, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
            if (entitySource==null) break;

            domainObject = DomainObjectUtils.buildDomainObject(dataSource, clazz, entitySource);

            //Необходима дополнительная проверка, так как нельзя исключать сломанный индекс или коллизии хеша
            boolean isFullCoincidence = true;
            for (Field field: structEntityIndex.indexFieldsSort) {
                Object filterFieldValue = filters.get(field.name());
                Object iFieldValue = domainObject.get(field.type(), field.name());
                if (!EqualsUtils.equals(filterFieldValue, iFieldValue)) {
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
