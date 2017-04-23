package com.infomaximum.rocksdb.transaction.engine.impl;

import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.exception.TimeoutLockException;
import com.infomaximum.rocksdb.transaction.Transaction;
import com.infomaximum.rocksdb.transaction.engine.EngineTransaction;
import com.infomaximum.rocksdb.transaction.engine.Monad;

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
     * @param monad Выполняемая операция.
     */
    public void execute(final Monad monad) {
        execute(monad, DEFAULT_RETRIES);
    }

    /**
     * Внутренняя реализация операции с данными.
     *
     * @param operation Выполняемая операция.
     */
    public void execute(final Monad operation, int retries) {
        final Transaction transaction = dataSource.createTransaction();
        int attempt = 0;
        do {
            // пытаемся выполнить операцию некоторое количество раз
            try {
                operation.action(transaction);
                transaction.commit();
                return;
            } catch (TimeoutLockException ex) {
                attempt += 1;
                try {
                    Thread.sleep(RETRY_TIMEOUT*attempt);
                } catch (InterruptedException ignored) {}
            } catch (Exception ex) {
                throw new RuntimeException("Exception execute transaction", ex);
            }
        } while (attempt<retries);
        throw new RuntimeException("Exception execute transaction");
    }

}
