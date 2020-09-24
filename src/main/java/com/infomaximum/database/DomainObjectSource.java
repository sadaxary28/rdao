package com.infomaximum.database;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.dbstruct.DBSchema;

public class DomainObjectSource {

    private final DBProvider dbProvider;
    private final DBSchema dbSchema;

    @FunctionalInterface
    public interface Monad {

        /**
         * Реализация операции.
         * @param transaction Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        void action(final Transaction transaction) throws Exception;
    }

    public DomainObjectSource(DBProvider dbProvider) throws DatabaseException {
        this.dbProvider = dbProvider;
        this.dbSchema = Schema.read(dbProvider).getDbSchema();
    }

    public void executeTransactional(final Monad operation) throws Exception {
        try (Transaction transaction = buildTransaction()) {
            operation.action(transaction);
            transaction.commit();
        }
    }

    public Transaction buildTransaction() throws DatabaseException {
        return new Transaction(dbProvider.beginTransaction(), dbSchema);
    }

    public RecordIterator select(String table, String namespace) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace);
    }

//    public DBIterator createIterator(String columnFamily) throws DatabaseException {
//
//        return dbProvider.createIterator(columnFamily);
//    }
//
//    public boolean isMarkedForDeletion(StructEntity entity, long objId) {
//        return false;
//    }
}
