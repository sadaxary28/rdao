package com.infomaximum.database.domainobject;

import com.infomaximum.database.core.schema.EntityField;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.domainobject.key.FieldKey;
import com.infomaximum.database.exeption.DataSourceDatabaseException;
import com.infomaximum.database.utils.TypeConvert;

public class DomainObjectSource extends DataEnumerable {

    public interface Monad {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        public void action(final Transaction transaction) throws Exception;
    }

    public DomainObjectSource(DataSource dataSource) {
        super(dataSource);
    }

    public void executeTransactional(final Monad operation) throws Exception {
        try (Transaction transaction = buildTransaction()) {
            operation.action(transaction);
            transaction.commit();
        }
    }

    public Transaction buildTransaction() {
        return new Transaction(dataSource);
    }

    @Override
    public <T, U extends DomainObject> T getValue(final EntityField field, U object) throws DataSourceDatabaseException {
        byte[] value = dataSource.getValue(object.getStructEntity().getColumnFamily(), new FieldKey(object.getId(), field.getName()).pack());
        return (T) TypeConvert.unpack(field.getType(), value, field.getPacker());
    }

    @Override
    public long createIterator(String columnFamily) throws DataSourceDatabaseException {
        return dataSource.createIterator(columnFamily);
    }
}
