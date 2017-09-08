package com.infomaximum.database.exeption;

/**
 * Created by kris on 08.09.17.
 */
public class TransactionDatabaseException extends DatabaseException  {

    public TransactionDatabaseException(String message) {
        super(message);
    }

    public TransactionDatabaseException(Throwable cause) {
        super(cause);
    }

    public TransactionDatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
