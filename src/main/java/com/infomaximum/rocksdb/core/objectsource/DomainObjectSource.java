package com.infomaximum.rocksdb.core.objectsource;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.core.lazyiterator.IteratorEntity;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodFilterImpl;
import com.infomaximum.rocksdb.core.objectsource.proxy.MethodHandlerImpl;
import com.infomaximum.rocksdb.core.objectsource.utils.DomainObjectUtils;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.HashStructEntities;
import com.infomaximum.rocksdb.core.objectsource.utils.structentity.StructEntity;
import com.infomaximum.rocksdb.core.struct.DomainObject;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.engine.impl.EngineTransactionImpl;
import com.infomaximum.rocksdb.utils.TypeConvertRocksdb;
import javassist.util.proxy.MethodFilter;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import org.rocksdb.RocksDBException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

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
    public <T extends DomainObject> T create(final Transaction transaction, final Class<? extends DomainObject> clazz) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.create(dataSource, transaction, clazz);
    }

    /**
     * load object abd lock object to write
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T edit(final Transaction transaction, final Class<? extends DomainObject> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.edit(dataSource, transaction, clazz, id);
    }

    /**
     * load object to readonly
     * @param id
     * @param <T>
     * @return
     */
    public <T extends DomainObject> T get(final Class<? extends DomainObject> clazz, long id) throws ReflectiveOperationException, RocksDBException {
        return DomainObjectUtils.get(dataSource, clazz, id);
    }

    /**
     * Возврощаем итератор по объектам
     * @param clazz
     * @param <T>
     * @return
     */
    public <T extends DomainObject> IteratorEntity<T> iterator(final Class<? extends DomainObject> clazz) {
        return new IteratorEntity(dataSource, clazz);
    }
}
