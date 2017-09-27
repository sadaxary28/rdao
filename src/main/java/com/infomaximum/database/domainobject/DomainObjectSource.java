package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.iterator.IteratorEntityImpl;
import com.infomaximum.database.core.iterator.IteratorFindEntityImpl;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.structentity.StructEntityIndex;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.EngineTransaction;
import com.infomaximum.database.core.transaction.engine.EngineTransactionImpl;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.domainobject.key.Key;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 19.04.2017.
 */
public class DomainObjectSource {

    private final DataSource dataSource;
    private final EngineTransaction engineTransaction;

    public DomainObjectSource(DataSource dataSource) {
        this.dataSource = dataSource;
        this.engineTransaction = new EngineTransactionImpl(dataSource);
    }

    public EngineTransaction getEngineTransaction() {
        return engineTransaction;
    }

    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        try {
            Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);

            long id = dataSource.nextId(entityAnnotation.name());

            T domainObject = DomainObjectUtils.buildDomainObject(dataSource, clazz, new EntitySource(id, null));

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (Field field: entityAnnotation.fields()) {
                domainObject.set(field.name(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final T domainObject, final Transaction transaction) throws DatabaseException {
        transaction.update(domainObject.getStructEntity(), domainObject, domainObject.getLoadValues(), domainObject.writeValues());
        domainObject.flush();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final T domainObject, final Transaction transaction) throws DatabaseException {
        transaction.remove(domainObject.getStructEntity(), domainObject);
    }

    public <T extends DomainObject> T get(final Class<T> clazz, long id) throws DataSourceDatabaseException {
        Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);

        long iteratorId = dataSource.createIterator(entityAnnotation.name(), FieldKey.getKeyPrefix(id));
        EntitySource entitySource;

        try {
            entitySource = DomainObjectUtils.nextEntitySource(dataSource, iteratorId, null);
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        return entitySource != null ? DomainObjectUtils.buildDomainObject(dataSource, clazz, entitySource) : null;
    }

    public <T extends DomainObject> T find(final Class<T> clazz, String filterFieldName, Object filterValue) throws DatabaseException {
        Map<String, Object> filters = new HashMap<>();
        filters.put(filterFieldName, filterValue);
        return find(clazz, filters);
    }

    public <T extends DomainObject> IteratorEntity<T> findAll(final Class<T> clazz, String filterFieldName, Object filterValue) throws DatabaseException {
        Map<String, Object> filters = new HashMap<>();
        filters.put(filterFieldName, filterValue);
        return findAll(clazz, filters);
    }

    public <T extends DomainObject> T find(final Class<T> clazz, Map<String, Object> filters) throws DatabaseException {
        try (IteratorFindEntityImpl iterator = new IteratorFindEntityImpl(dataSource, clazz, filters)) {
            return iterator.hasNext() ? (T) iterator.next() : null;
        }
    }

    public <T extends DomainObject> IteratorEntity<T> findAll(final Class<T> clazz, Map<String, Object> filters) throws DatabaseException {
        return new IteratorFindEntityImpl(dataSource, clazz, filters);
    }

    public <T extends DomainObject> IteratorEntity<T> iterator(final Class<T> clazz) throws DatabaseException {
        return new IteratorEntityImpl<>(dataSource, clazz);
    }

    public <T extends DomainObject> void createEntity(final Class<T> clazz) throws DatabaseException {
        StructEntity entity = new StructEntity(clazz);
        dataSource.createColumnFamily(entity.annotationEntity.name());
        dataSource.createSequence(entity.annotationEntity.name());
        for (StructEntityIndex i : entity.getStructEntityIndices()) {
            dataSource.createColumnFamily(i.columnFamily);
        }

        //TODO realize
    }
}
