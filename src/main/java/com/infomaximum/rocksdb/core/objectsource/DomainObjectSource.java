package com.infomaximum.rocksdb.core.objectsource;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.iterator.IteratorEntity;
import com.infomaximum.rocksdb.core.iterator.IteratorFindEntity;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.engine.impl.EngineTransactionImpl;
import org.rocksdb.RocksDBException;

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
    public <T extends DomainObject> T create(final Transaction transaction, final Class<T> clazz) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.create(dataSource, transaction, clazz);
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T get(final Class<T> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.get(dataSource, null, clazz, id);
    }

    /**
     * load object abd lockObject object to write
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T edit(final Transaction transaction, final Class<T> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.get(dataSource, transaction, clazz, id);
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T find(final Class<T> clazz, String fieldName, Object value) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.find(dataSource, null, clazz, fieldName, value);
    }

    /**
     * find object to readonly
     * @param <T>
     * @return
     */
    public <T extends DomainObject> IteratorFindEntity<T> findAll(final Class<T> clazz, String fieldName, Object value) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.findAll(dataSource, null, clazz, fieldName, value);
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
}
