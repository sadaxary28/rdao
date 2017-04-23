package com.infomaximum.rocksdb.transaction.engine;

/**
 * Created by user on 23.04.2017.
 */
public interface EngineTransaction {

    /**
     * Количество попыток выполнения операции, по умолчанию 5 раза.
     */
    final static int DEFAULT_RETRIES = 5;


    /**
     * Время ожидания между попытками выполнить операцию в миллисекундах, по умолчанию 100 мс.
     */
    final static int RETRY_TIMEOUT = 100;


    /**
     * Внутренняя реализация операции с данными.
     *
     * @param operation Выполняемая операция.
     */
    public void execute(final Monad operation);


    /**
     * Внутренняя реализация операции с данными.
     *
     * @param operation Выполняемая операция.
     */
    public void execute(final Monad operation, int retries);
}
