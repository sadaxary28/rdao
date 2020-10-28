package com.infomaximum.database;

import com.infomaximum.database.domainobject.filter.HashFilter;
import com.infomaximum.database.domainobject.filter.IntervalFilter;
import com.infomaximum.database.domainobject.filter.PrefixFilter;
import com.infomaximum.database.domainobject.filter.RangeFilter;
import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBProvider;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.Schema;
import com.infomaximum.database.schema.dbstruct.DBSchema;

public class RecordSource {

    private final DBProvider dbProvider;
    private final DBSchema dbSchema;

    @FunctionalInterface
    public interface Monad {

        /**
         * Реализация операции.
         * @param dataCommand Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        void action(final DataCommand dataCommand) throws Exception;
    }

    @FunctionalInterface
    public interface Function<R> {

        /**
         * Реализация операции.
         * @param dataCommand Контекст, в котором выполняется операция.
         * @throws Exception Если во время выполнения операции возникла ошибка.
         */
        R apply(final DataCommand dataCommand) throws Exception;
    }

    public RecordSource(DBProvider dbProvider) throws DatabaseException {
        this.dbProvider = dbProvider;
        this.dbSchema = Schema.read(dbProvider).getDbSchema();
    }

    public RecordIterator select(String table, String namespace) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace);
    }

    public RecordIterator select(String table, String namespace, HashFilter filter) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace, filter);
    }

    public RecordIterator select(String table, String namespace, PrefixFilter filter) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace, filter);
    }

    public RecordIterator select(String table, String namespace, IntervalFilter filter) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace, filter);
    }

    public RecordIterator select(String table, String namespace, RangeFilter filter) throws DatabaseException {
        return new DataReadCommand(dbProvider, dbSchema).select(table, namespace, filter);
    }

    public void executeTransactional(final Monad operation) throws Exception {
        try (DBTransaction transaction = dbProvider.beginTransaction()) {
            operation.action(buildDataCommand(transaction));
            transaction.commit();
        }
    }

    public <R> R executeTransactional(final Function<R> function) throws Exception {
        try (DBTransaction transaction = dbProvider.beginTransaction()) {
            R result = function.apply(buildDataCommand(transaction));
            transaction.commit();
            return result;
        }
    }

    private DataCommand buildDataCommand(DBTransaction transaction) {
        return new DataCommand(transaction, dbSchema);
    }
}
