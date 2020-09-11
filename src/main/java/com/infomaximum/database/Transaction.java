package com.infomaximum.database;

import com.infomaximum.database.exception.DatabaseException;
import com.infomaximum.database.provider.DBTransaction;
import com.infomaximum.database.schema.dbstruct.DBSchema;

public class Transaction implements AutoCloseable {

    private final DBTransaction dbTransaction;
    private final DataCommand dataCommand;

    Transaction(DBTransaction dbTransaction, DBSchema schema) {
        this.dbTransaction = dbTransaction;
        this.dataCommand = new DataCommand(dbTransaction, schema);
    }

    public DataCommand getCommand() {
        return dataCommand;
    }

    public void commit() throws DatabaseException {
        dbTransaction.commit();
    }

    public void rollback() throws DatabaseException {
        dbTransaction.rollback();
    }

    @Override
    public void close() throws DatabaseException {
        dbTransaction.close();
    }
}
