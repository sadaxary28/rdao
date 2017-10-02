package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.core.iterator.IteratorEntity;
import com.infomaximum.database.core.iterator.IteratorEntityImpl;
import com.infomaximum.database.core.iterator.IteratorFindEntityImpl;
import com.infomaximum.database.core.schema.StructEntity;
import com.infomaximum.database.core.schema.EntityIndex;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.datasource.KeyPattern;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.exeption.DatabaseException;
import com.infomaximum.database.exeption.TransactionDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by user on 19.04.2017.
 */
public class DomainObjectSource implements DataEnumerable {

    public interface Monad {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        public void action(final Transaction transaction) throws Exception;
    }

    private final DataSource dataSource;

    public DomainObjectSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void executeTransactional(final Monad operation) throws TransactionDatabaseException {
        try (Transaction transaction = buildTransaction()) {
            operation.action(transaction);
            transaction.commit();
        } catch (Exception ex) {
            throw new TransactionDatabaseException("Exception execute transaction", ex);
        }
    }

    public Transaction buildTransaction() {
        return new Transaction(dataSource);
    }

    @Override
    public <T extends Object, U extends DomainObject> T getValue(final EntityField field, U object) throws DataSourceDatabaseException {
        byte[] value = dataSource.getValue(object.getStructEntity().getName(), new FieldKey(object.getId(), field.getName()).pack());
        return (T) TypeConvert.unpack(field.getType(), value, field.getPacker());
    }

    @Override
    public <T extends DomainObject> T get(final Class<T> clazz, final Set<String> loadingFields, long id) throws DataSourceDatabaseException {
        String columnFamily = StructEntity.getInstance(clazz).getName();
        KeyPattern pattern = FieldKey.buildKeyPattern(id, loadingFields != null ? loadingFields : Collections.emptySet());

        long iteratorId = dataSource.createIterator(columnFamily, pattern);

        T obj;
        try {
            obj = DomainObjectUtils.nextObject(clazz, dataSource, iteratorId, this, null);
        } finally {
            dataSource.closeIterator(iteratorId);
        }

        return obj;
    }

    @Override
    public <T extends DomainObject> IteratorEntity<T> find(final Class<T> clazz, final Set<String> loadingFields, Map<String, Object> filters) throws DatabaseException {
        return new IteratorFindEntityImpl(dataSource, this, clazz, loadingFields, filters, -1);
    }

    @Override
    public <T extends DomainObject> IteratorEntity<T> iterator(final Class<T> clazz, final Set<String> loadingFields) throws DatabaseException {
        return new IteratorEntityImpl(dataSource, this, clazz, loadingFields, -1);
    }

    public <T extends DomainObject> void createEntity(final Class<T> clazz) throws DatabaseException {
        StructEntity entity = StructEntity.getInstance(clazz);
        dataSource.createColumnFamily(entity.getName());
        dataSource.createSequence(entity.getName());
        for (EntityIndex i : entity.getIndices()) {
            dataSource.createColumnFamily(i.columnFamily);
        }

        //TODO realize
    }
}
