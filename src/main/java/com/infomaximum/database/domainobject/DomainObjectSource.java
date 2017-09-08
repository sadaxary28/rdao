package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.anotation.Entity;
import com.infomaximum.database.core.anotation.Field;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.iterator.IteratorFindEntity;
import com.infomaximum.database.core.structentity.HashStructEntities;
import com.infomaximum.database.core.structentity.StructEntity;
import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.EngineTransaction;
import com.infomaximum.database.core.transaction.engine.impl.EngineTransactionImpl;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.entitysource.EntitySource;
import com.infomaximum.database.datasource.entitysource.EntitySourceImpl;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.utils.TypeConvert;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Constructor;
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

    /**
     * create object
     * @param <T>
     * @return
     */
    public <T extends DomainObject & DomainObjectEditable> T create(final Class<T> clazz) throws DatabaseException {
        try {
            Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);

            long id = dataSource.nextId(entityAnnotation.name());

            T domainObject = createDomainObject(clazz, new EntitySourceImpl(id, null));

            //Принудительно указываем, что все поля отредактированы - иначе для не инициализированных полей не правильно построятся индексы
            for (Field field: entityAnnotation.fields()) {
                domainObject.set(field.name(), null);
            }

            return domainObject;
        } catch (Exception e) {
            throw new DatabaseException(e);
        }
    }

    public <T extends DomainObject & DomainObjectEditable> void save(final Transaction transaction, final T domainObject) {
        transaction.update(domainObject.getStructEntity(), domainObject, domainObject.getLoadValues(), domainObject.writeValues());
        domainObject.flush();
    }

    public <T extends DomainObject & DomainObjectEditable> void remove(final Transaction transaction, final T domainObject) throws ReflectiveOperationException, RocksDBException {
        transaction.remove(domainObject.getStructEntity(), domainObject);
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T get(final Class<T> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        Entity entityAnnotation = StructEntity.getEntityAnnotation(clazz);

        EntitySource entitySource = dataSource.getEntitySource(entityAnnotation.name(), false, id, HashStructEntities.getStructEntity(clazz).getEagerFormatFieldNames());
        if (entitySource==null) return null;

        T domainObject = createDomainObject(clazz, entitySource);

        return domainObject;
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T find(final Class<T> clazz, String fieldName, Object value) throws ReflectiveOperationException, RocksDBException {
        IteratorFindEntity iteratorFindEntity = new IteratorFindEntity(dataSource, clazz, new HashMap<String, Object>(){{ put(fieldName, value); }});
        if (iteratorFindEntity.hasNext()) {
            return (T) iteratorFindEntity.next();
        } else {
            return null;
        }
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> IteratorFindEntity<T> findAll(final Class<T> clazz, String fieldName, Object value) throws ReflectiveOperationException, RocksDBException {
        return new IteratorFindEntity(dataSource, clazz, new HashMap<String, Object>(){{ put(fieldName, value); }});
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T find(final Class<T> clazz, Map<String, Object> filters) throws ReflectiveOperationException, RocksDBException {
        IteratorFindEntity iteratorFindEntity = new IteratorFindEntity(dataSource, clazz, filters);
        if (iteratorFindEntity.hasNext()) {
            return (T) iteratorFindEntity.next();
        } else {
            return null;
        }
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> IteratorFindEntity<T> findAll(final Class<T> clazz, Map<String, Object> filters) throws ReflectiveOperationException, RocksDBException {
        return new IteratorFindEntity(dataSource, clazz, filters);
    }

    /**
     * Возврощаем итератор по объектам
     * @param clazz
     * @param <T>
     * @return
     */
    public <T extends DomainObject> IteratorEntity<T> iterator(final Class<T> clazz) throws RocksDBException, ReflectiveOperationException {
        return new IteratorEntity<>(dataSource, clazz);
    }

    private <T extends DomainObject> T createDomainObject(final Class<T> clazz, EntitySource entitySource) throws ReflectiveOperationException, RocksDBException {
        Constructor<T> constructor = clazz.getConstructor(long.class);

        T domainObject = constructor.newInstance(entitySource.getId());

        //Устанавливаем dataSource
        HashStructEntities.getDataSourceField().set(domainObject, dataSource);

        //Загружаем поля
        Map<String, byte[]> data = entitySource.getFields();
        if (data!=null) {
            StructEntity structEntity = domainObject.getStructEntity();

            for (Field field: structEntity.getFields()) {
                String fieldName = field.name();
                if (data.containsKey(fieldName)) {
                    byte[] bValue = data.get(fieldName);
                    Object value = TypeConvert.get(field.type(), bValue);
                    domainObject.set(fieldName, value);
                }
            }
        }

        return domainObject;
    }
}
