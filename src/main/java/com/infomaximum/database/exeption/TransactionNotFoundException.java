package com.infomaximum.database.exeption;

public class TransactionNotFoundException extends DataSourceDatabaseException {

    public TransactionNotFoundException(long transactionId) {
        super(String.format("Transaction #%d not found.", transactionId));
    }
}