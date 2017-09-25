package com.infomaximum.database.core.transaction.engine;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.exeption.TransactionDatabaseException;

/**
 * Created by user on 23.04.2017.
 */
public interface EngineTransaction {
    /**
     * Внутренняя реализация операции с данными.
     *
     * @param operation Выполняемая операция.
     */
    public void execute(final Monad operation) throws TransactionDatabaseException;

    public Transaction createTransaction();

}
