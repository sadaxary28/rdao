package com.infomaximum.database.core.transaction.engine.impl;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.database.core.transaction.engine.EngineTransaction;
import com.infomaximum.database.core.transaction.engine.Monad;
import com.infomaximum.database.datasource.DataSource;
import com.infomaximum.database.exeption.TransactionDatabaseException;

/**
 * Created by user on 23.04.2017.
 */
public class EngineTransactionImpl implements EngineTransaction {

    private DataSource dataSource;

    public EngineTransactionImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Внутренняя реализация операции с данными.
     *
     * @param operation Выполняемая операция.
     */
    public void execute(final Monad operation) throws TransactionDatabaseException {
        final Transaction transaction = new Transaction(dataSource);
        int attempt = 0;
        do {
            // пытаемся выполнить операцию некоторое количество раз
            try {
                operation.action(transaction);
                transaction.commit();
                return;
            } catch (RuntimeException ex) {
                attempt += 1;
                try {
                    Thread.sleep(RETRY_TIMEOUT*attempt);
                } catch (InterruptedException ignored) {}
            } catch (Exception ex) {
                throw new TransactionDatabaseException("Exception execute transaction", ex);
            }
        } while (attempt < DEFAULT_RETRIES);
        throw new TransactionDatabaseException("Exception execute transaction");
    }

    @Override
    public Transaction createTransaction() {
        return new Transaction(dataSource);
    }

}
