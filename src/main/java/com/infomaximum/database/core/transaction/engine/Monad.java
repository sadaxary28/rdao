package com.infomaximum.database.core.transaction.engine;

import com.infomaximum.database.core.transaction.Transaction;

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
