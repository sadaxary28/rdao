package com.infomaximum.database.core.transaction.engine;

import com.infomaximum.database.core.transaction.Transaction;
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
    @Override
    public void execute(final Monad operation) throws TransactionDatabaseException {
        try (Transaction transaction = new Transaction(dataSource)) {
            operation.action(transaction);
            transaction.commit();
        } catch (Exception ex) {
            throw new TransactionDatabaseException("Exception execute transaction", ex);
        }
    }

    @Override
    public Transaction createTransaction() {
        return new Transaction(dataSource);
    }

}
