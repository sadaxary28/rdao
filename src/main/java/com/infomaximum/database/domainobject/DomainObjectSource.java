package com.infomaximum.database.domainobject;

import com.infomaximum.database.provider.DBIterator;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.schema.Field;
import com.infomaximum.database.utils.key.FieldKey;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.utils.TypeConvert;

public class DomainObjectSource extends DataEnumerable {

    public interface Monad {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        void action(final Transaction transaction) throws Exception;
    }

    public DomainObjectSource(DBProvider dbProvider) {
        super(dbProvider);
    }

    public void executeTransactional(final Monad operation) throws Exception {
        try (Transaction transaction = buildTransaction()) {
            operation.action(transaction);
            transaction.commit();
        }
    }

    public Transaction buildTransaction() {
        return new Transaction(dbProvider);
    }

    @Override
    public <T, U extends DomainObject> T getValue(final Field field, U object) throws DatabaseException {
        byte[] value = dbProvider.getValue(object.getStructEntity().getColumnFamily(), new FieldKey(object.getId(), field.getNameBytes()).pack());
        return (T) TypeConvert.unpack(field.getType(), value, field.getConverter());
    }

    @Override
    public DBIterator createIterator(String columnFamily) throws DatabaseException {
        return dbProvider.createIterator(columnFamily);
    }
}
