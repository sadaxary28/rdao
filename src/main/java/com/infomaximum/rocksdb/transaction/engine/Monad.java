package com.infomaximum.rocksdb.transaction.engine;

import com.infomaximum.rocksdb.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Единица работы с данными - атомарная операция, выполняющаяся в контексте изоляции транзакции,
 * обычно с оптимистичной блокировкой.
 *
 * @author jatvarthur
 */
public interface Monad {


	/**
	 * Реализация операции.
	 *
	 * @param transaction Контекст, в котором выполняется операция.
	 * @throws Exception Если во время выполнения операции возникла ошибка.
	 */
	public void action(final Transaction transaction) throws Exception;

}
